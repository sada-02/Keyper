package store

import (
	"github.com/dgraph-io/badger/v4"
)

// Iterator provides a simple forward iterator over the Badger store.
// Usage:
//
//	it := st.NewIterator()
//	defer it.Close()
//	for it.Rewind(); it.Valid(); it.Next() {
//	    k := it.Key()
//	    v := it.Value()
//	    ...
//	}
type Iterator struct {
	txn  *badger.Txn
	it   *badger.Iterator
	item *badger.Item

	// cached key/value copies
	key []byte
	val []byte
}

func (s *BadgerStore) NewIterator() *Iterator {
	txn := s.db.NewTransaction(false) // read-only
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	it := txn.NewIterator(opts)
	iter := &Iterator{
		txn: txn,
		it:  it,
	}
	// position before the first element; user should call Rewind()
	return iter
}

// Rewind positions iterator at the first key.
func (it *Iterator) Rewind() {
	it.it.Rewind()
	it.advance()
}

func (it *Iterator) advance() {
	if !it.it.Valid() {
		it.item = nil
		it.key = nil
		it.val = nil
		return
	}
	it.item = it.it.Item()
	if it.item == nil {
		it.key = nil
		it.val = nil
		return
	}
	// copy key and value to avoid transaction dependency
	it.key = it.item.KeyCopy(nil)
	v, err := it.item.ValueCopy(nil)
	if err != nil {
		it.val = nil
	} else {
		it.val = v
	}
}

// Valid returns true if the iterator is currently pointing at a valid item.
func (it *Iterator) Valid() bool {
	return it.item != nil
}

// Next advances to the next item.
func (it *Iterator) Next() {
	it.it.Next()
	it.advance()
}

// Key returns a copy of the current key.
func (it *Iterator) Key() []byte {
	return it.key
}

// Value returns a copy of the current value.
func (it *Iterator) Value() []byte {
	return it.val
}

// Close closes the iterator and discards the transaction.
func (it *Iterator) Close() {
	if it.it != nil {
		it.it.Close()
	}
	if it.txn != nil {
		it.txn.Discard()
	}
	it.item = nil
	it.key = nil
	it.val = nil
}
