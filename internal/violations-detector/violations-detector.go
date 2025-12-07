package violations_detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/danilaavduskin-bit/testTask/config"
	"github.com/kvolis/tesgode/cat"
	"github.com/kvolis/tesgode/dog"
	"github.com/kvolis/tesgode/models"
	"log"
	"sync/atomic"
	"time"
)

type Storage interface {
	Insert(key string, value []byte) (int, error)
	Close() error
	Connect(conn string) error
}

type Detector struct {
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
		return fmt.Errorf("[%s]Cat subscription error: %w", op, err)
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
		log.Printf("Message %d processed, Invalid JSON (from Cat): %v", atomic.LoadInt32(&d.processed), err)
		return nil
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
		log.Printf("Message %d processing", atomic.LoadInt32(&d.processed))
		return d.saveViolation(passage.LicenseNum, passage.Track[maxIndex])
	}
	log.Printf("Message %d processed, Violations not detected", atomic.LoadInt32(&d.processed))
	return nil
}

func (d *Detector) saveViolation(license string, point models.TPoint) error {
	const op = "violations_detector.saveViolation"

	key := fmt.Sprintf("%s_%s", license, time.Unix(int64(point.T), 0).Format("2006-01-02_15:04:05"))
	value := fmt.Sprintf(`{"license":"%s","time":%d,"x":%f,"y":%f}`, license, point.T, point.X, point.Y)

	for i := 1; i <= d.maxRetries; i++ {
		_, err := d.dogClient.Insert(key, []byte(value))
		if nil == err {
			return nil
		}

		if !errors.Is(err, dog.ErrInternal) {
			return fmt.Errorf("[%s]Insert error: %w", op, err)
		}

		log.Printf("%v", err)

		if i < d.maxRetries {
			delay := time.Duration(i) * 200 * time.Millisecond
			log.Printf("Retry %d/%d for %s (wait %v)", i, d.maxRetries, license, delay)
			time.Sleep(delay)
			continue
		}
	}

	log.Printf("Failed to save violation after %d attempts: %s", d.maxRetries, license)
	return nil
}
