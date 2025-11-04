package store

import (
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// kvJSON is the line object we write/read: base64-encoded key/value.
type kvJSON struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// ExportToWriter writes the entire store contents as gzipped ndjson of base64-encoded k/v pairs.
func ExportToWriter(s *BadgerStore, w io.Writer) error {
	gw := gzip.NewWriter(w)
	enc := json.NewEncoder(gw)

	it := s.NewIterator()
	if it == nil {
		_ = gw.Close()
		return fmt.Errorf("iterator creation failed")
	}
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Key()
		v := it.Value()
		entry := kvJSON{
			Key:   base64.StdEncoding.EncodeToString(k),
			Value: base64.StdEncoding.EncodeToString(v),
		}
		if err := enc.Encode(&entry); err != nil {
			_ = gw.Close()
			return fmt.Errorf("encode entry: %w", err)
		}
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("close gzip: %w", err)
	}
	return nil
}

// ImportFromReader reads gzipped ndjson produced by ExportToWriter and writes it into the provided store.
// It uses Badger WriteBatch for efficiency.
func ImportFromReader(s *BadgerStore, r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	dec := json.NewDecoder(gr)

	// Use Badger's WriteBatch for efficient bulk writes
	wb := s.db.NewWriteBatch()
	defer wb.Cancel() // Cancel will be no-op if already flushed

	// flush periodically
	const flushEvery = 1000
	count := 0

	for {
		var entry kvJSON
		if err := dec.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode entry: %w", err)
		}
		k, err := base64.StdEncoding.DecodeString(entry.Key)
		if err != nil {
			return fmt.Errorf("decode key base64: %w", err)
		}
		v, err := base64.StdEncoding.DecodeString(entry.Value)
		if err != nil {
			return fmt.Errorf("decode value base64: %w", err)
		}
		if err := wb.Set(k, v); err != nil {
			return fmt.Errorf("writebatch set: %w", err)
		}
		count++
		if count%flushEvery == 0 {
			if err := wb.Flush(); err != nil {
				return fmt.Errorf("writebatch flush: %w", err)
			}
			// create a fresh writebatch
			wb.Cancel()
			wb = s.db.NewWriteBatch()
		}
	}

	// final flush
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("final writebatch flush: %w", err)
	}
	// small pause to give badger time (optional)
	time.Sleep(50 * time.Millisecond)
	return nil
}
