package main

import (
	"testing"
	"time"
)

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

	timestamp := uint64(time.Now().Unix())
	entry := InitEntry([]byte(key), value, timestamp)

	if err := store.writeEntry(entry, key, value, timestamp); err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
}

func assertKeyInKeyDir(t *testing.T, store *Store, key string) LatestEntryRecord {
	t.Helper()

	record, exists := store.KeyDir[key]
	if !exists {
		t.Fatalf("key %s not found in KeyDir", key)
	}

	return record
}
