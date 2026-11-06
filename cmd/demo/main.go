// A small demo: enqueues fake emails, about 30% of the sends fail and get retried.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/pgqueue"
)

const queueName = "emails"

type email struct {
	To string `json:"to"`
}

func main() {
	jobs := flag.Int("jobs", 100, "how many jobs to enqueue")
	concurrency := flag.Int("concurrency", 8, "number of worker goroutines")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	client := pgqueue.New(pool)
	if err := client.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *jobs; i++ {
		_, err := client.Enqueue(ctx, pgqueue.EnqueueParams{
			Queue:   queueName,
			Kind:    "send_email",
			Payload: email{To: fmt.Sprintf("user%d@example.com", i)},
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	worker, err := pgqueue.NewWorker(client, pgqueue.WorkerConfig{
		Queue:        queueName,
		Concurrency:  *concurrency,
		PollInterval: 100 * time.Millisecond,
		Backoff:      func(attempt int) time.Duration { return time.Duration(attempt) * 100 * time.Millisecond },
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Handlers:     map[string]pgqueue.HandlerFunc{"send_email": sendEmail},
	})
	if err != nil {
		log.Fatal(err)
	}

	go printStats(ctx, stop, client)

	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func sendEmail(ctx context.Context, job *pgqueue.Job) error {
	var e email
	if err := json.Unmarshal(job.Payload, &e); err != nil {
		return err
	}
	time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)
	if rand.Float64() < 0.3 {
		return errors.New("smtp: connection reset")
	}
	return nil
}

// printStats stops the demo when the queue is empty.
func printStats(ctx context.Context, stop func(), client *pgqueue.Client) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := client.Stats(ctx, queueName)
			if err != nil {
				continue
			}
			fmt.Printf("queued=%-5d running=%-3d done=%-5d dead=%d\n",
				stats["queued"], stats["running"], stats["done"], stats["dead"])
			if stats["queued"] == 0 && stats["running"] == 0 {
				stop()
				return
			}
		}
	}
}
