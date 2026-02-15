package store

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
)

func withMockFS(s *store) {
	if s != nil {
		s.fileSystem = MockFileSystem{}
	}
}
func withFileSystem(fs FileSystem) option {
	return func(s *store) {
		s.fileSystem = fs
	}
}

func openTestROStore(t *testing.T, tempDir string, options ...option) *ROStore {
	t.Helper()

	store, _ := OpenReadOnly(tempDir, withOsFileSystem, options[0])

	t.Cleanup(func() {
		store.Close()
	})

	return store

}
func openTestRWStore(t *testing.T, syncOnPut bool, tempDir string) *RWStore {
	t.Helper()

	store, _ := Open(tempDir, syncOnPut, withOsFileSystem)

	t.Cleanup(func() {
		store.Close()
	})

	return store

}

func setupRWTestStore(t *testing.T, syncOnPut bool) (*RWStore, string) {
	t.Helper()

	tempDir := t.TempDir()
	store, _ := Open(tempDir, syncOnPut, withOsFileSystem)

	t.Cleanup(func() {
		store.Close()
	})

	return store, tempDir
}
func setupROTestStore(t *testing.T, syncOnPut bool) (*ROStore, string) {
	t.Helper()

	tempDir := t.TempDir()
	store, _ := OpenReadOnly(tempDir)

	t.Cleanup(func() {
		store.Close()
	})

	return store, tempDir
}

func assertFileSize(t *testing.T, path string, size int, message string) {
	fileStat, _ := os.Stat(path)
	if size != int(fileStat.Size()) {
		t.Errorf(message, size, fileStat.Size())
	}
}

func assertInList[T comparable](t *testing.T, entry T, list []T, message string) {
	if slices.Index(list, entry) == -1 {
		t.Errorf(message, entry)
	}
}
func assertNotInList[T comparable](t *testing.T, entry T, list []T, message string) {
	if slices.Index(list, entry) != -1 {
		t.Errorf(message, entry)
	}
}

// Warning! During testing, generated timestamps will be same. Because of this, stale, and updated entries will be treated as same.
// Use writeTestEntryWithTimeStamp tp have sequential entry timestamps
func writeTestEntry(t *testing.T, store *RWStore, key string, value []byte) {
	t.Helper()

	if err := store.Put(key, value); err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
}
func writeTestEntryWithTimeStamp(t *testing.T, store *RWStore, key string, value []byte, timestamp uint64) {
	t.Helper()

	entry := initEntry([]byte(key), value, timestamp)

	store.mu.Lock()
	defer store.mu.Unlock()
	record, err := store.writeEntry(entry, key, timestamp)
	store.currentSize += uint32(len(entry))

	if err != nil {
		t.Fatalf("writeEntry failed: %v", err)
	}
	store.KeyDir[key] = *record
}

func assertKeyInKeyDir(t *testing.T, store *store, key string) EntryRecord {
	t.Helper()

	record, exists := store.KeyDir[key]
	if !exists {
		t.Fatalf("key %s not found in KeyDir", key)
	}

	return record
}

// Ensures that current active file becomes inactive (forces file rotation), by Puting "blank" entries.
// Setting isTemp to false will make sure that all inserted "blank" entries get persisted after merge operation
// Setting isTemp to true will guarantee that at least one entry will be preserved after merge operation
// keySuffix is keyvalue that will be used for entries. In the case of isTemp false entry keys are formatted: %dkeySuffix
//
// Returns: number of entries inserted, and latest written entry key
func ensureFileRotationHappens(t *testing.T, store *RWStore, isTemp bool, keySuffix string) (uint32, int) {
	sizeOfEntryValue := 1 << 30 // 1GB
	temp := float64(MAXIMUM_FILE_SIZE-store.currentSize) / float64(sizeOfEntryValue)
	numberOfEntriesNeeded := math.Ceil(temp)
	var entryKey string
	for i := 0; i < int(numberOfEntriesNeeded); i++ {

		entryKey = fmt.Sprintf("%d%s", i, keySuffix)
		if isTemp {
			entryKey = keySuffix
		}

		writeTestEntry(t, store, entryKey, make([]byte, sizeOfEntryValue))
	}
	return uint32(numberOfEntriesNeeded), sizeOfEntryValue
}

func assertEntryExistsKeyValue(t *testing.T, store *store, key, value string) {
	t.Helper()
	v, err := store.Get(key)
	if err != nil {
		t.Errorf("Unexpected Get error key:%s err:%v", key, err)
	}
	if string(v) != value {
		t.Errorf("For key:%s expected value: %s, but got %s", key, value, string(v))
	}
}

func assertKeyNotInKeyDir(t *testing.T, store *store, key string) {
	t.Helper()
	_, exists := store.KeyDir[key]
	if exists {
		t.Fatalf("unexpected key: %s found in KeyDir", key)
	}
}

func mockEntriesByFile(valueSizes []int, keySize int) map[int][]MergeEntryRecord {
	entries := make(map[int][]MergeEntryRecord)
	for i, vs := range valueSizes {
		fileId := i % 3 // spread across 3 files arbitrarily
		entries[fileId] = append(entries[fileId], mockEntry(keySize, vs))
	}
	return entries
}

// mockEntry builds a single MergeEntryRecord with given key and value sizes.
func mockEntry(keySize, valueSize int) MergeEntryRecord {
	return MergeEntryRecord{
		Key: strings.Repeat("k", keySize),
		Record: EntryRecord{
			KeySize:   uint32(keySize),
			ValueSize: uint32(valueSize),
		},
	}
}

// groupSize sums the encoded size of all entries in a group.
func groupSize(group []MergeEntryRecord) int {
	return fold(group, 0, func(acc int, e MergeEntryRecord) int {
		return acc + int(e.Record.KeySize) + int(e.Record.ValueSize) + HEADER_SIZE
	})
}
