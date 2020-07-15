package dfq

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	q, err := Open("./test/queue")
	if err != nil {
		t.Error(err)
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

	f, err := q.Peek()
	if err != nil {
		t.Error(err)
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
