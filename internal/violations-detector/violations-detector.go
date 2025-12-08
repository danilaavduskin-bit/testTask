package violations_detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/danilaavduskin-bit/testTask/config"
	"github.com/kvolis/tesgode/cat"
	"github.com/kvolis/tesgode/dog"
	"github.com/kvolis/tesgode/models"
)

// Правильный подход - разделить код на слои, но исходя из вводных условий задачи
// решил остановиться на таком варианте, готов к рефакторингу если потребуется
type Storage interface {
	Insert(key string, value []byte) (int, error)
	Close() error
	Connect(conn string) error
}

type Detector struct {
	// Правильный тон - взаимодействовать через абстракции, но придется усложнять код через ввод адаптера для Cat
	catClient   *cat.Cat
	dogClient   Storage
	maxMessages int32
	processed   int32
	violations  int32
	maxRetries  int
}

func NewDetector(catClient *cat.Cat, dogClient Storage, cfg *config.Config) *Detector {
	return &Detector{
		catClient:   catClient,
		dogClient:   dogClient,
		maxMessages: cfg.MaxMessages,
		maxRetries:  cfg.MaxRetries,
	}
}

func (d *Detector) Run(ctx context.Context) error {
	const op = "violations_detector.Run"

	ch, err := d.catClient.Subscript()
	if err != nil {
		return fmt.Errorf("%s: subscription failed: %w", op, err)
	}

	log.Println("Service started, waiting for messages...")

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}

			if err := d.handleMessage(msg); err != nil {
				log.Printf("Handle message error: %v", err)
			}
			if atomic.LoadInt32(&d.processed) >= d.maxMessages {
				log.Printf("Processed %d messages, found %d violations, stopping", d.processed, d.violations)
				return nil
			}
		}
	}
}

func (d *Detector) handleMessage(msg cat.Message) error {
	const op = "violations_detector.handleMessage"

	atomic.AddInt32(&d.processed, 1)

	var passage models.Passage
	if err := json.Unmarshal(msg.Bytes(), &passage); err != nil {
		return fmt.Errorf("%s: message %d: invalid JSON: %w", op, atomic.LoadInt32(&d.processed), err)
	}

	if len(passage.Track) == 0 {
		return nil
	}

	maxIndex := 0
	for i := 1; i < len(passage.Track); i++ {
		if passage.Track[i].T > passage.Track[maxIndex].T {
			maxIndex = i
		}
	}

	sec := time.Unix(int64(passage.Track[maxIndex].T), 0).Second()
	if sec >= 45 && sec <= 59 {
		atomic.AddInt32(&d.violations, 1)

		if err := d.saveViolation(passage.LicenseNum, passage.Track[maxIndex].T); err != nil {
			return fmt.Errorf("%s: save failed: %w", op, err)
		}
	} else {
		log.Printf("Message %d processed, Violations not detected", atomic.LoadInt32(&d.processed))
	}

	return nil
}

func (d *Detector) saveViolation(license string, T int) error {
	const op = "violations_detector.saveViolation"

	key := fmt.Sprintf("%s_%s", license, time.Unix(int64(T), 0).Format("2006-01-02_15:04:05"))
	value := fmt.Sprintf(`{"license":"%s","time":%d}`, license, T)

	for i := 1; i <= d.maxRetries; i++ {
		_, err := d.dogClient.Insert(key, []byte(value))
		// Специально пишу наоборот, чтобы глаз не перепутал с err != nil
		if nil == err {
			return nil
		}

		if !errors.Is(err, dog.ErrInternal) {
			return fmt.Errorf("%s: insert error: %w", op, err)
		}

		if i < d.maxRetries {
			delay := time.Duration(i) * 200 * time.Millisecond
			log.Printf("Retry %d/%d for %s (wait %v)", i, d.maxRetries, license, delay)
			time.Sleep(delay)
			continue
		}
	}

	return fmt.Errorf("%s: unexpected error", op)
}
