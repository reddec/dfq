package dfq

import (
	"context"
	"io"
)

// Queue designed for multiple writers and single consumer
type Queue interface {
	// Put data from stream to queue. Could be run concurrently
	Put(reader io.Reader) error
	// Steal item from queue by copying item to the back of self queue and committing in original one.
	// Has internal optimization for file-based queues if they are on the same devices/partitions.
	Steal(from Queue) error
	// Stream data to new queue entity. Entity will be automatically added to queue after finish without error.
	Stream(handler func(out io.Writer) error) error
	// Peek oldest item or return an error (ErrEmptyQueue). Can be called concurrently,
	// but reader should close stream manually and strictly before commit
	Peek() (io.ReadCloser, error)
	// Commit and discard current (oldest) item
	Commit() error
	// Peek oldest record or wait for new one
	Wait(ctx context.Context) (io.ReadCloser, error)
	// Remove allocated resources
	Destroy() error
	// Number of elements in queue
	Len() int64
}

type emptyQueue int

func (eq *emptyQueue) Error() string { return "Empty Queue" }

// Empty queue error
var ErrEmptyQueue = new(emptyQueue)
