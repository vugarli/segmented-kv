package main

import (
	"testing"
)

func openTestStore(t *testing.T, syncOnPut bool, tempDir string) *Store {
	t.Helper()

	store, err := Open(tempDir, OSFileSystem{}, syncOnPut)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		store.Close()
	})

	return store

}

func setupTestStore(t *testing.T, syncOnPut bool) (*Store, string) {
	t.Helper()

	tempDir := t.TempDir()
	store, err := Open(tempDir, OSFileSystem{}, syncOnPut)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		store.Close()
	})

	return store, tempDir
}

func writeTestEntry(t *testing.T, store *Store, key string, value []byte) {
	t.Helper()

	if err := store.Put(key, value); err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
}
func writeTestEntryWithTimeStamp(t *testing.T, store *Store, key string, value []byte, timestamp uint64) {
	t.Helper()

	entry := InitEntry([]byte(key), value, timestamp)

	store.mu.Lock()
	defer store.mu.Unlock()
	record, err := store.writeEntry(entry, key, value, timestamp)

	if err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
	store.KeyDir[key] = *record
}

func assertKeyInKeyDir(t *testing.T, store *Store, key string) LatestEntryRecord {
	t.Helper()

	record, exists := store.KeyDir[key]
	if !exists {
		t.Fatalf("key %s not found in KeyDir", key)
	}

	return record
}

func assertKeyNotInKeyDir(t *testing.T, store *Store, key string) {
	t.Helper()
	_, exists := store.KeyDir[key]
	if exists {
		t.Fatalf("unexpected key: %s found in KeyDir", key)
	}
}
