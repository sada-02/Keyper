package store

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/dgraph-io/badger/v4"
)

var (
	// ErrNotFound returned when a key is not present.
	ErrNotFound = errors.New("key not found")
)

// BadgerStore wraps a Badger DB instance with a minimal API.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore opens/creates a Badger DB at the given dir.
func NewBadgerStore(dir string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(dir)
	// Disable verbose logging (applications may set a logger here).
	opts.Logger = nil
	// Ensure writes are synced to disk (durable).
	opts.SyncWrites = true

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: db}, nil
}

// Close closes the underlying DB.
func (s *BadgerStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Get reads a value for a key. Returns ErrNotFound if missing.
func (s *BadgerStore) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		valCopy = v
		return nil
	})
	return valCopy, err
}

// Set writes key -> value (overwrite if exists).
func (s *BadgerStore) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(&badger.Entry{
			Key:   key,
			Value: value,
		})
	})
}

// Delete removes a key. If key not present, returns ErrNotFound.
func (s *BadgerStore) Delete(key []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(key); err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			return err
		}
		return nil
	})
	return err
}

// KVPair is the on-disk/export JSON format for snapshots.
type KVPair struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// Export writes the entire DB as newline-separated JSON KVPair objects to w.
// Caller is responsible for choosing how to persist/stream w (file, socket, etc).
func (s *BadgerStore) Export(w io.Writer) error {
	enc := json.NewEncoder(w)
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			kv := KVPair{
				Key:   string(k),
				Value: v,
			}
			if err := enc.Encode(&kv); err != nil {
				return err
			}
		}
		return nil
	})
}

// Import reads newline-separated JSON KVPair objects from r and writes them into the DB.
// It will overwrite existing keys with the values read.
func (s *BadgerStore) Import(r io.Reader) error {
	dec := json.NewDecoder(r)
	for {
		var kv KVPair
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := s.Set([]byte(kv.Key), kv.Value); err != nil {
			return err
		}
	}
}
