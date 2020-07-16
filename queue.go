package dfq

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	dataSuffix = ".data"
	tempSuffix = ".temp"
)

// Open file-based queue
func Open(directory string) (*queue, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}
	q := &queue{
		directory: directory,
		notify:    make(chan struct{}, 1),
	}
	return q, q.synchronizeState()
}

// Single-process, file based queue, where one file is one record.
//
// It's designed to have multiple writers and one reader
type queue struct {
	directory string
	notify    chan struct{}
	reader    struct {
		lock      sync.Mutex
		currentId int64
	}
	writer struct {
		lock    sync.Mutex
		counter int64
	}
}

// Put data from stream to file. Could be run concurrently
func (q *queue) Put(reader io.Reader) error {
	return q.Stream(func(stream io.Writer) error {
		_, err := io.Copy(stream, reader)
		if err != nil {
			return fmt.Errorf("dfq: put: write temp file: %w", err)
		}
		return nil
	})
}

// Stream data to new queue entity. Entity will be automatically added to queue after finish without error.
func (q *queue) Stream(handler func(out io.Writer) error) error {
	tmp, err := ioutil.TempFile(q.directory, "*.temp")
	if err != nil {
		return fmt.Errorf("dfq: put: create temp file: %w", err)
	}
	err = handler(tmp)
	_ = tmp.Close()
	if err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	err = q.attachToQueue(tmp.Name())
	if err != nil {
		return fmt.Errorf("dfq: attach to queue: %w", err)
	}
	q.notifyUpdate()
	return nil
}

// Peek oldest file or return ErrNotExist. Can be called concurrently,
// but reader should close stream manually and strictly before commit
func (q *queue) Peek() (io.ReadCloser, error) {
	q.reader.lock.Lock()
	defer q.reader.lock.Unlock()
	f, err := os.Open(filepath.Join(q.directory, fmt.Sprint(q.reader.currentId, dataSuffix)))
	if err == nil {
		return f, nil
	}
	if os.IsNotExist(err) {
		return nil, ErrEmptyQueue
	}
	return nil, err
}

// Commit current file: remove it from FS and move reader sequence forward
func (q *queue) Commit() error {
	q.reader.lock.Lock()
	path := filepath.Join(q.directory, fmt.Sprint(q.reader.currentId, dataSuffix))
	q.reader.currentId++
	q.reader.lock.Unlock()
	q.notifyUpdate()
	err := os.Remove(path)
	return err
}

// Peek oldest record or wait for new one
func (q *queue) Wait(ctx context.Context) (io.ReadCloser, error) {
	for {
		f, err := q.Peek()
		if err == nil {
			return f, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.notify:
		}
	}
}

// Remove everything in a queue directory (and directory itself)
func (q *queue) Destroy() error {
	return os.RemoveAll(q.directory)
}

func (q *queue) attachToQueue(oldName string) error {
	q.writer.lock.Lock()
	defer q.writer.lock.Unlock()
	id := q.writer.counter + 1
	err := os.Rename(oldName, filepath.Join(q.directory, fmt.Sprint(id, dataSuffix)))
	if err != nil {
		return fmt.Errorf("rename temp file to queue element: %w", err)
	}
	q.writer.counter = id
	return nil
}

func (q *queue) notifyUpdate() {
	select {
	case q.notify <- struct{}{}:
	default:

	}
}

func (q *queue) synchronizeState() error {
	list, err := ioutil.ReadDir(q.directory)
	if err != nil {
		return err
	}
	var min int64 = 1
	var max int64
	for _, file := range list {
		name := file.Name()
		if strings.HasSuffix(name, dataSuffix) {
			id, err := strconv.ParseInt(name[:len(name)-len(dataSuffix)], 10, 64)
			if err != nil {
				return err
			}
			if id > max {
				max = id
			} else if id < min {
				min = id
			}
		} else if strings.HasSuffix(name, tempSuffix) {
			err = os.Remove(filepath.Join(q.directory, name))
			if err != nil {
				return err
			}
		}
	}
	q.reader.currentId = min
	q.writer.counter = max
	return nil
}

// Helper for queue to peek and read full stream and return bytes
func GetBytes(q interface {
	Peek() (io.ReadCloser, error)
}) ([]byte, error) {
	f, err := q.Peek()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ioutil.ReadAll(f)
}

// Helper for queue to peek and read full stream and return as a string
func GetString(q interface {
	Peek() (io.ReadCloser, error)
}) (string, error) {
	data, err := GetBytes(q)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
