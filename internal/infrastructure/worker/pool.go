package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueFull indicates the worker pool channel is saturated (backpressure).
	ErrQueueFull = errors.New("worker pool queue is full")

	// ErrPoolStopped indicates jobs cannot be submitted because the pool is shutting down.
	ErrPoolStopped = errors.New("worker pool is stopped")
)

// Job represents a unit of concurrent work executed by a pool worker.
type Job func(ctx context.Context)

// WorkerPool manages a bounded queue of jobs executed by fixed number of goroutines.
type WorkerPool struct {
	workers       int
	queueCapacity int
	jobs          chan Job
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	stopped       atomic.Bool
	activeWorkers atomic.Int32
}

// NewWorkerPool instantiates a worker pool with specified concurrency and buffer capacity.
func NewWorkerPool(workers, queueCapacity int) *WorkerPool {
	if workers <= 0 {
		workers = 16
	}
	if queueCapacity <= 0 {
		queueCapacity = 5000
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		workers:       workers,
		queueCapacity: queueCapacity,
		jobs:          make(chan Job, queueCapacity),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start spawns the worker goroutines.
func (p *WorkerPool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}
}

func (p *WorkerPool) workerLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining queued jobs before exiting cleanly
			for job := range p.jobs {
				p.executeJob(job)
			}
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			p.executeJob(job)
		}
	}
}

func (p *WorkerPool) executeJob(job Job) {
	p.activeWorkers.Add(1)
	defer p.activeWorkers.Add(-1)

	defer func() {
		if r := recover(); r != nil {
			// Catch panic in user job to prevent worker termination
			_ = fmt.Sprintf("job recovered from panic: %v", r)
		}
	}()

	job(p.ctx)
}

// Submit enqueues a job for asynchronous execution, returning ErrQueueFull if channel is full.
func (p *WorkerPool) Submit(job Job) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

// QueueDepth returns the count of items currently pending in the job channel.
func (p *WorkerPool) QueueDepth() int {
	return len(p.jobs)
}

// ActiveWorkers returns the count of workers currently executing a job.
func (p *WorkerPool) ActiveWorkers() int {
	return int(p.activeWorkers.Load())
}

// Stop gracefully halts the worker pool, draining remaining jobs with a timeout.
func (p *WorkerPool) Stop(timeout time.Duration) error {
	if !p.stopped.CompareAndSwap(false, true) {
		return nil
	}

	p.cancel()
	close(p.jobs)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("worker pool drain timed out after %v", timeout)
	}
}
