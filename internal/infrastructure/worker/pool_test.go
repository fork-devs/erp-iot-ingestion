package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_ExecutionAndDrain(t *testing.T) {
	pool := NewWorkerPool(4, 100)
	pool.Start()

	var counter atomic.Int32
	jobsCount := 50

	for i := 0; i < jobsCount; i++ {
		err := pool.Submit(func(ctx context.Context) {
			counter.Add(1)
		})
		if err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	err := pool.Stop(2 * time.Second)
	if err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}

	if counter.Load() != int32(jobsCount) {
		t.Errorf("expected %d jobs completed, got %d", jobsCount, counter.Load())
	}
}

func TestWorkerPool_QueueFullBackpressure(t *testing.T) {
	// 1 worker, buffer capacity 2
	pool := NewWorkerPool(1, 2)
	// Do not start worker immediately so jobs stay queued
	blocker := make(chan struct{})

	_ = pool.Submit(func(ctx context.Context) {
		<-blocker
	})
	_ = pool.Submit(func(ctx context.Context) {})

	// Third job may succeed or fail depending on buffer, but fourth definitely fails
	_ = pool.Submit(func(ctx context.Context) {})
	err := pool.Submit(func(ctx context.Context) {})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected ErrQueueFull on saturated channel, got %v", err)
	}

	pool.Start()
	close(blocker)
	_ = pool.Stop(time.Second)
}
