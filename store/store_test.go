package store_test

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/sada-02/keyper/store"
)

func TestBadgerStoreBasicCRUD(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "dkvs_test_basic_"+strconv.FormatInt(int64(os.Getpid()), 10))
	defer os.RemoveAll(dir)

	s, err := store.NewBadgerStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	key := []byte("k1")
	val := []byte("v1")

	// get missing -> ErrNotFound
	if _, err := s.Get(key); err == nil {
		t.Fatalf("expected not found for missing key")
	}

	if err := s.Set(key, val); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	got, err := s.Get(key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("unexpected value: got=%s want=%s", got, val)
	}

	if err := s.Delete(key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	if _, err := s.Get(key); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

func TestBadgerStoreConcurrency(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "dkvs_test_conc_"+strconv.FormatInt(int64(os.Getpid()), 10))
	defer os.RemoveAll(dir)

	s, err := store.NewBadgerStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	const goroutines = 8
	const opsPer = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPer; i++ {
				k := []byte("k-" + strconv.Itoa(id) + "-" + strconv.Itoa(i%20))
				v := []byte("v-" + strconv.Itoa(i))
				if err := s.Set(k, v); err != nil {
					t.Errorf("set err: %v", err)
					return
				}
				if _, err := s.Get(k); err != nil {
					t.Errorf("get err: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
