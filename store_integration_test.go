package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPut(t *testing.T) {

	t.Run("Successful write", func(t *testing.T) {
		mf := OSFileSystem{}
		key, value := "key", "value"
		directory := t.TempDir()
		store, _ := Open(directory, mf, true)

		if err := store.Put(key, []byte(value)); err != nil {
			t.Errorf("Write failed %v", err)
		}

		store.Close()

		entryFile, _ := os.ReadFile(filepath.Join(directory, "0.data"))

		extractedKey, _ := ExtractKey(entryFile)
		extractedValue, _ := ExtractValue(entryFile)

		if string(extractedKey) != key {
			t.Errorf("Expected %s, but got %s as a key", key, extractedKey)
		}
		if string(extractedValue) != value {
			t.Errorf("Expected %s, but got %s as a key", value, &extractedValue)
		}

		if VerifyEntryCRC(entryFile) != nil {
			t.Error("CRC check failed!")
		}

	})

	t.Run("Successful write with correct position", func(t *testing.T) {
		store, directory := setupTestStore(t, false)

		writeTestEntry(t, store, "key1", []byte("value1"))
		writeTestEntry(t, store, "key2", []byte("value2"))

		record := assertKeyInKeyDir(t, store, "key1")

		if record.ValueSize != 6 {
			t.Errorf("wrong ValueSize: got %d, want 6", record.ValueSize)
		}
		store.Close()

		entryFile, _ := os.ReadFile(filepath.Join(directory, "0.data"))

		firstEntry := entryFile[:HEADER_SIZE+10]
		secondEntry := entryFile[HEADER_SIZE+10:]

		if err := VerifyEntryCRC(firstEntry); err != nil {
			t.Errorf("FirstEntry: CRC verification failed: %v", err)
		}

		if err := VerifyEntryCRC(secondEntry); err != nil {
			t.Errorf("SecondEntry: CRC verification failed: %v", err)
		}

		fV, _ := ExtractValue(firstEntry)
		sV, _ := ExtractValue(secondEntry)

		if string(fV) != "value1" {
			t.Error("First entry value is wrong")
		}

		if string(sV) != "value2" {
			t.Error("Second entry value is wrong")
		}
	})
	t.Run("multiple writes to same key", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := Open(tempDir, OSFileSystem{}, false)
		defer store.Close()

		key := "key"

		store.Put(key, []byte("value1"))

		record1 := store.KeyDir[key]

		store.Put(key, []byte("value2"))

		record2 := store.KeyDir[key]

		if record1.ValuePos == record2.ValuePos {
			t.Error("expected different positions for overwrites")
		}

	})

	t.Run("concurrent writes are serialized", func(t *testing.T) {
		tempDir := t.TempDir()
		store, _ := Open(tempDir, OSFileSystem{}, false)
		defer store.Close()

		var wg sync.WaitGroup
		numWrites := 100

		for i := 0; i < numWrites; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", n)
				value := []byte(fmt.Sprintf("value%d", n))
				store.Put(key, value)
			}(i)
		}

		wg.Wait()

		if len(store.KeyDir) != numWrites {
			t.Errorf("expected %d keys, got %d", numWrites, len(store.KeyDir))
		}
	})

	t.Run("Successful write with correct currentSize", func(t *testing.T) {
		store, _ := setupTestStore(t, false)

		writeTestEntry(t, store, "key1", []byte("value1"))
		writeTestEntry(t, store, "key2", []byte("value2"))
		writeTestEntry(t, store, "key3", []byte("value3"))

		record := assertKeyInKeyDir(t, store, "key1")

		if record.ValueSize != 6 {
			t.Errorf("wrong ValueSize: got %d, want 6", record.ValueSize)
		}
		gotSize := store.currentSize
		store.Close()

		expectedSize := 3 * (HEADER_SIZE + 4 + 6)

		if uint32(expectedSize) != gotSize {
			t.Errorf("Store.currentSize is wrong. Expected size: %d, but got %d", expectedSize, gotSize)
		}
	})
}

func TestFileRotation(t *testing.T) {
	t.Run("successful rotation after size exceeds", func(t *testing.T) {
		store, directory := setupTestStore(t, false)

		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))
		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))
		writeTestEntry(t, store, "1gb", make([]byte, 1<<30))

		if store.currentFileId != 1 {
			t.Error("Expected file rotation didn't happen.")
			return
		}

		store.Close()

		oldFile, _ := os.Stat(path.Join(directory, "0.data"))
		newFile, _ := os.Stat(path.Join(directory, "1.data"))

		if oldFile.Size() != 1<<31+2*(HEADER_SIZE+3) {
			t.Errorf("Old data file size is wrong, expected: %d, got: %d", 1<<31, oldFile.Size())
		}
		if newFile.Size() != 1<<30+HEADER_SIZE+3 {
			t.Errorf("New data file size is wrong, expected: %d, got: %d", 1<<30, newFile.Size())
		}
	})
}

func TestWriteEntry_ValuePosition(t *testing.T) {
	tempDir := t.TempDir()
	store, _ := Open(tempDir, OSFileSystem{}, false)
	defer store.Close()

	tests := []struct {
		key   string
		value []byte
	}{
		{"a", []byte("x")},
		{"short", []byte("value")},
		{"medium-length-key", []byte("some value here")},
		{strings.Repeat("k", 100), []byte("value")},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			positionBefore, _ := store.currentFile.Seek(0, io.SeekCurrent)

			err := store.Put(tt.key, tt.value)
			if err != nil {
				t.Fatal(err)
			}

			record := store.KeyDir[tt.key]

			expectedValuePos := positionBefore + int64(HEADER_SIZE) + int64(len(tt.key))

			if record.ValuePos != uint64(expectedValuePos) {
				t.Errorf("wrong ValuePos: got %d, want %d", record.ValuePos, expectedValuePos)
			}

			store.currentFile.Seek(int64(record.ValuePos), io.SeekStart)
			valueRead := make([]byte, record.ValueSize)
			store.currentFile.Read(valueRead)

			if !bytes.Equal(valueRead, tt.value) {
				t.Error("value at ValuePos doesn't match")
			}
		})
	}
}

func TestUpdateKeydir(t *testing.T) {

	entries := []struct {
		key      string
		value    string
		expected string
		pos      uint64
	}{
		{key: "key", value: "value1", expected: "value3", pos: 136},
		{key: "XX", value: "value", expected: "value", pos: 51},
		{key: "key", value: "value2", expected: "value3", pos: 136},
		{key: "XXX", value: "value", expected: "value", pos: 108},
		{key: "key", value: "value3", expected: "value3", pos: 136},

		{key: "XXX", value: "value3", expected: "value3", pos: 23},
		{key: "key", value: "value4", expected: "value4", pos: 51},
	}

	store, directory := setupTestStore(t, true)

	for i, entry := range entries[:5] {
		writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
	}

	store.Close()

	store = openTestStore(t, true, directory)

	for _, entry := range entries[:5] {
		record := assertKeyInKeyDir(t, store, entry.key)
		if record.ValuePos != entry.pos {
			t.Errorf("Value position is wrong for %s. Expected %d but found %d", entry.key, record.ValuePos, uint64(HEADER_SIZE+len(entry.key)))
		}
		value, err := store.Get(entry.key)
		if err != nil {
			t.Errorf("Error %v", err)
		}
		if string(value) != entry.expected {
			t.Errorf("Expected %s: %s, but got %s", entry.key, entry.value, string(value))
		}

	}

	for i, entry := range entries[5:] {
		writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
	}

	for _, entry := range entries[5:] {
		assertKeyInKeyDir(t, store, entry.key)
		value, err := store.Get(entry.key)
		if err != nil {
			t.Errorf("Error %v", err)
		}
		if string(value) != entry.expected {
			t.Errorf("Expected %s: %s, but got %s", entry.key, entry.value, string(value))
		}

	}

}

func TestDelete(t *testing.T) {
	t.Run("Remove entry successfully", func(t *testing.T) {

		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, _ := setupTestStore(t, true)

		for i, entry := range entries[:5] {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}

		deletedKey := "key"
		err := store.Delete(deletedKey)
		if err != nil {
			t.Errorf("Failed to delete entry with key:%s", deletedKey)
		}

		for _, entry := range entries {
			if entry.key == deletedKey {
				assertKeyNotInKeyDir(t, store, entry.key)
			}
		}
	})
	t.Run("Removed entry shouldn't be present when store opens again", func(t *testing.T) {
		entries := []struct {
			key   string
			value string
		}{
			{key: "key", value: "value1"},
			{key: "XX", value: "value"},
			{key: "key", value: "value2"},
			{key: "XXX", value: "value"},
			{key: "key", value: "value3"},
		}

		store, directory := setupTestStore(t, true)

		for i, entry := range entries[:5] {
			writeTestEntryWithTimeStamp(t, store, entry.key, []byte(entry.value), uint64(i))
		}

		deletedKey := "key"
		err := store.Delete(deletedKey)
		if err != nil {
			t.Errorf("Failed to delete entry with key:%s", deletedKey)
		}

		store.Close()
		store = openTestStore(t, true, directory)

		for _, entry := range entries {
			if entry.key == deletedKey {
				assertKeyNotInKeyDir(t, store, entry.key)
			}
		}
	})

}
