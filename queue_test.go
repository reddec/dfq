package dfq

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	q, err := Open("./test/queue")
	if err != nil {
		t.Error(err)
		return
	}
	if q.Len() != 0 {
		t.Error("non-empty queue")
		return
	}
	err = q.Put(bytes.NewBufferString("var1"))
	if err != nil {
		t.Error(err)
		return
	}
	err = q.Put(bytes.NewBufferString("var2"))
	if err != nil {
		t.Error(err)
		return
	}
	if q.Len() != 2 {
		t.Error("queue size should 2")
		return
	}
	f, err := q.Peek()
	if err != nil {
		t.Error(err)
		return
	}
	if q.Len() != 2 {
		t.Error("queue size should 2")
		return
	}
	data, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		t.Error(err)
		return
	}

	if string(data) != "var1" {
		t.Error("corrupted")
		return
	}

	dataAgain, err := GetBytes(q)
	if err != nil {
		t.Error(err)
		return
	}

	if string(dataAgain) != string(data) {
		t.Error("moved uncommitted")
		return
	}

	err = q.Commit()
	if err != nil {
		t.Error(err)
		return
	}
	if q.Len() != 1 {
		t.Error("queue size should 1")
		return
	}

	if s, err := GetString(q); err != nil {
		t.Error(err)
		return
	} else if s != "var2" {
		t.Error("corrupted")
		return
	}

	err = q.Commit()
	if err != nil {
		t.Error(err)
		return
	}

	_, err = q.Peek()
	if !errors.Is(err, ErrEmptyQueue) {
		t.Error("has to be no file")
	}

	go func() {
		<-time.After(1 * time.Second)
		if err := q.Put(bytes.NewBufferString("hello world")); err != nil {
			t.Error(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	v, err := q.Wait(ctx)
	cancel()
	if err != nil {
		t.Error(err)
		return
	}

	data, err = ioutil.ReadAll(v)
	if err != nil {
		t.Error(err)
		return
	}

	if string(data) != "hello world" {
		t.Error("corrupted data")
		return
	}

	// re-open

	q2, err := Open("./test/queue")
	if err != nil {
		t.Error(err)
		return
	}
	if q.Len() != 1 {
		t.Error("queue size should 1")
		return
	}
	v, err = q2.Peek()
	if err != nil {
		t.Error(err)
		return
	}
	_ = v.Close()

	err = q.Destroy()
	if err != nil {
		t.Error(err)
	}
}

func TestQueue_Steal(t *testing.T) {
	q, err := Open("./test/queue1")
	if err != nil {
		t.Error(err)
		return
	}
	defer q.Destroy()

	q2, err := Open("./test/queue2")
	defer q2.Destroy()

	err = q.Put(bytes.NewBufferString("hello"))
	if err != nil {
		t.Error(err)
		return
	}

	err = q2.Steal(q)
	if err != nil {
		t.Error(err)
		return
	}

	if q.Len() != 0 {
		t.Errorf("original queue should be empty")
	}
	if q2.Len() != 1 {
		t.Errorf("target queue should be filled")
	}

	v, err := GetString(q2)
	if err != nil {
		t.Error(err)
		return
	}
	if v != "hello" {
		t.Errorf("unknown message")
	}
}

func BenchmarkQueue_Put(b *testing.B) {
	q, err := Open("test/queue")
	if err != nil {
		b.Error(err)
		return
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := q.Put(bytes.NewBufferString("hello world"))
			if err != nil {
				b.Error(err)
			}
		}
	})
	b.StopTimer()
	_ = os.RemoveAll("test/queue")
}

func BenchmarkQueue_PeekCommit(b *testing.B) {
	q, err := Open("test/queue")
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		err := q.Put(bytes.NewBufferString("hello world"))
		if err != nil {
			b.Error(err)
			return
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := q.Peek()
		if err != nil {
			b.Error(err)
			return
		}
		_, err = io.Copy(ioutil.Discard, f)
		_ = f.Close()
		if err != nil {
			b.Error(err)
			return
		}
		err = q.Commit()
		if err != nil {
			b.Error(err)
			return
		}
	}
}
