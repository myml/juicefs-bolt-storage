package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
)

var (
	boltDefaultBucket = []byte("default")
)

func main() {
}

type boltStore struct {
	db *bolt.DB
}

func (b *boltStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	var r io.ReadCloser
	return r, b.db.View(func(t *bolt.Tx) error {
		b := t.Bucket(boltDefaultBucket)
		data := b.Get([]byte(key))
		if len(data) < int(off) {
			return io.EOF
		}
		r = io.NopCloser(bytes.NewReader(data[off:]))
		return nil
	})
}

func (b *boltStore) Put(key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return b.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(boltDefaultBucket)
		return b.Put([]byte(key), data)
	})
}
func (b *boltStore) Delete(key string) error {
	return b.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(boltDefaultBucket)
		return b.Delete([]byte(key))
	})
}

func (b *boltStore) String() string {
	return fmt.Sprintf("bolb://%s/", "")
}

func New(url, user, passwd string) (interface{}, error) {
	db, err := bolt.Open("bolt.db", 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists(boltDefaultBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &boltStore{db: db}, nil
}
