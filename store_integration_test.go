package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPut(t *testing.T) {

	t.Run("Successful write", func(t *testing.T) {
		mf := OSFileSystem{}
		key, value := "key", "value"
		directory := t.TempDir()
		store, _ := Open(directory, mf, true)

		timeStamp := uint64(time.Now().Unix())

		entry := InitEntry([]byte(key), []byte(value), timeStamp)

		if err := store.writeEntry(entry, key, []byte(value), timeStamp); err != nil {
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

		entry1 := InitEntry([]byte(key), []byte("value1"), 100)
		store.writeEntry(entry1, key, []byte("value1"), 100)

		record1 := store.KeyDir[key]

		entry2 := InitEntry([]byte(key), []byte("value2"), 200)
		store.writeEntry(entry2, key, []byte("value2"), 200)

		record2 := store.KeyDir[key]

		if record1.ValuePos == record2.ValuePos {
			t.Error("expected different positions for overwrites")
		}

		if record2.Timestamp != 200 {
			t.Errorf("timestamp not updated: got %d, want 200", record2.Timestamp)
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
				timestamp := uint64(n)
				entry := InitEntry([]byte(key), value, timestamp)

				store.writeEntry(entry, key, value, timestamp)
			}(i)
		}

		wg.Wait()

		if len(store.KeyDir) != numWrites {
			t.Errorf("expected %d keys, got %d", numWrites, len(store.KeyDir))
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
			timestamp := uint64(time.Now().Unix())
			entry := InitEntry([]byte(tt.key), tt.value, timestamp)

			positionBefore, _ := store.currentFile.Seek(0, io.SeekCurrent)

			err := store.writeEntry(entry, tt.key, tt.value, timestamp)
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
