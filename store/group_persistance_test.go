package store

import (
	"errors"
	"fmt"
	"testing"
)

func TestParseHintEntry(t *testing.T) {
	valueSize := 32
	valuePos := 64
	timestamp := 128
	key := "key"
	keySize := len(key)
	fileId := 23

	entryRecord := EntryRecord{
		FileId:    0,
		ValueSize: uint32(valueSize),
		ValuePos:  uint64(valuePos),
		Timestamp: uint64(timestamp),
		KeySize:   uint32(keySize),
	}

	mresult := MergeResult{StaleEntry: entryRecord,
		Key:      key,
		ValuePos: uint64(valuePos),
		FileId:   fileId}

	hintEntryByte := InitHintEntry(mresult)
	hintEntryHeader := parseHintEntryHeader(hintEntryByte)

	if hintEntryHeader.ValuePos != uint64(valuePos) {
		t.Errorf("Expected %v, but got %v", valuePos, hintEntryHeader.ValuePos)
	}
	if uint64(hintEntryHeader.ValueSize) != uint64(valueSize) {
		t.Errorf("Expected %v, but got %v", valueSize, hintEntryHeader.ValueSize)
	}
	if uint64(hintEntryHeader.Timestamp) != uint64(timestamp) {
		t.Errorf("Expected %v, but got %v", timestamp, hintEntryHeader.Timestamp)
	}
	if uint64(hintEntryHeader.KeySize) != uint64(keySize) {
		t.Errorf("Expected %v, but got %v", keySize, hintEntryHeader.KeySize)
	}
}

func TestCommitToDisk(t *testing.T) {
	t.Run("single file ID renames once", func(t *testing.T) {
		mock := &MockFileSystem{
			renames: make(map[string]string),
		}

		strat := CommitToDisk("\\data", mock)
		results := []MergeResult{
			{FileId: 1, Key: "a", ValuePos: 100},
			{FileId: 1, Key: "b", ValuePos: 200},
			{FileId: 1, Key: "c", ValuePos: 300},
		}

		strat(results)

		if len(mock.renames) != 1 {
			t.Errorf("expected 1 rename, got %d", len(mock.renames))
		}

		expected := "\\data\\1.data"
		got := mock.renames["\\data\\1.datatemp"]
		if got != expected {
			t.Errorf("expected rename to %s, got %s", expected, got)
		}
	})

	t.Run("multiple file IDs each renamed once", func(t *testing.T) {
		mock := &MockFileSystem{
			renames: make(map[string]string),
		}

		strat := CommitToDisk("\\data", mock)
		results := []MergeResult{
			{FileId: 1, Key: "a"},
			{FileId: 2, Key: "b"},
			{FileId: 3, Key: "c"},
			{FileId: 2, Key: "d"}, // duplicate file ID
			{FileId: 1, Key: "e"}, // duplicate file ID
		}

		strat(results)

		if len(mock.renames) != 3 {
			t.Errorf("expected 3 renames, got %d", len(mock.renames))
		}

		for fileId := 1; fileId <= 3; fileId++ {
			oldPath := fmt.Sprintf("\\data\\%d.datatemp", fileId)
			expectedNew := fmt.Sprintf("\\data\\%d.data", fileId)
			gotNew := mock.renames[oldPath]
			if gotNew != expectedNew {
				t.Errorf("file %d: expected %s, got %s", fileId, expectedNew, gotNew)
			}
		}
	})

	t.Run("empty results does nothing", func(t *testing.T) {
		mock := &MockFileSystem{
			renames: make(map[string]string),
		}

		strat := CommitToDisk("\\data", mock)
		strat([]MergeResult{})

		if len(mock.renames) != 0 {
			t.Errorf("expected no renames, got %d", len(mock.renames))
		}
	})

	t.Run("rename errors don't panic", func(t *testing.T) {
		mock := &MockFileSystem{
			renameErr: errors.New("permission denied"),
			renames:   make(map[string]string),
		}

		strat := CommitToDisk("\\data", mock)
		results := []MergeResult{
			{FileId: 1, Key: "a"},
		}

		// should not panic despite error
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("CommitToDisk panicked: %v", r)
			}
		}()

		strat(results)
	})

	t.Run("preserves order independence", func(t *testing.T) {
		// same file IDs in different order should produce same renames
		mock1 := &MockFileSystem{renames: make(map[string]string)}
		mock2 := &MockFileSystem{renames: make(map[string]string)}

		results1 := []MergeResult{
			{FileId: 3, Key: "a"},
			{FileId: 1, Key: "b"},
			{FileId: 2, Key: "c"},
		}
		results2 := []MergeResult{
			{FileId: 1, Key: "b"},
			{FileId: 2, Key: "c"},
			{FileId: 3, Key: "a"},
		}

		CommitToDisk("\\data", mock1)(results1)
		CommitToDisk("\\data", mock2)(results2)

		if len(mock1.renames) != len(mock2.renames) {
			t.Errorf("order affected result count: %d vs %d", len(mock1.renames), len(mock2.renames))
		}
	})
}
