package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		syncOnPut     bool
		expectedError error
		setupMock     func(*MockFileSystem)
	}{
		{
			name:          "Directory doesn't exist",
			path:          "/nonexistent",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Path is file not directory",
			path:          "/file.txt",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, syscall.ENOTDIR
				}
			},
		},
		{
			name:          "No read permission",
			path:          "/restricted",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "No write permission",
			path:          "/readonly",
			syncOnPut:     false,
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "Successful open without sync",
			path:          "/valid/store",
			syncOnPut:     false,
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, nil
				}
				m.RemoveFunc = func(name string) error {
					return nil
				}
				m.acquireExclusiveLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
		{
			name:          "Successful open with sync",
			path:          "/valid/store",
			syncOnPut:     true,
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, nil
				}
				m.RemoveFunc = func(name string) error {
					return nil
				}
				m.acquireExclusiveLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &MockFileSystem{}
			tt.setupMock(mockFS)

			store, err := Open(tt.path, mockFS, tt.syncOnPut)

			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tt.expectedError)
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("Expected error %v, got %v", tt.expectedError, err)
				}
				if store != nil {
					t.Errorf("Expected nil store on error, got %+v", store)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if store == nil {
					t.Fatal("Expected non-nil store, got nil")
				}
				if store.DirectoryName != tt.path {
					t.Errorf("Expected directory %s, got %s", tt.path, store.DirectoryName)
				}
				if store.KeyDir == nil {
					t.Errorf("Expected initialized KeyDir, got nil")
				}
				if store.syncOnPut != tt.syncOnPut {
					t.Errorf("Expected syncOnPut %v, got %v", tt.syncOnPut, store.syncOnPut)
				}
			}
		})
	}
}
func TestOpenReadOnly(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		expectedError error
		setupMock     func(*MockFileSystem)
	}{
		{
			name:          "Directory doesn't exist",
			path:          "/nonexistent",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Path is file not directory",
			path:          "/file.txt",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, syscall.ENOTDIR
				}
			},
		},
		{
			name:          "No read permission",
			path:          "/restricted",
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "Successful read-only open",
			path:          "/valid/store",
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
				m.acquireSharedLockFunc = func(directory string) (*os.File, error) {
					return nil, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &MockFileSystem{}
			tt.setupMock(mockFS)

			store, err := OpenReadOnly(tt.path, mockFS)

			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("Expected error %v, got nil", tt.expectedError)
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("Expected error %v, got %v", tt.expectedError, err)
				}
				if store != nil {
					t.Errorf("Expected nil store on error, got %+v", store)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
				if store == nil {
					t.Fatal("Expected non-nil store, got nil")
				}
				if store.DirectoryName != tt.path {
					t.Errorf("Expected directory %s, got %s", tt.path, store.DirectoryName)
				}
				if store.KeyDir == nil {
					t.Errorf("Expected initialized KeyDir, got nil")
				}
			}
		})
	}
}

func TestInitEntry(t *testing.T) {
	cases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "Correct key value",
			key:   []byte("key"),
			value: []byte("value"),
		},
		{
			name:  "Empty key value",
			key:   make([]byte, 0),
			value: make([]byte, 0),
		},
		{
			name:  "large key",
			key:   []byte(strings.Repeat("k", 1024)),
			value: []byte("value"),
		},
		{
			name:  "large value",
			key:   []byte("key"),
			value: make([]byte, 1024*1024),
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			entry := InitEntry(tt.key, tt.value, uint64(time.Now().Unix()))

			expectedSize := HEADER_SIZE + len(tt.key) + len(tt.value)
			if len(entry) != expectedSize {
				t.Errorf("entry size = %d, want %d", len(entry), expectedSize)
			}

			keySize := binary.LittleEndian.Uint32(entry[KEY_SIZE_OFFSET:])
			if keySize != uint32(len(tt.key)) {
				t.Errorf("keySize = %d, want %d", keySize, len(tt.key))
			}

			valueSize := binary.LittleEndian.Uint32(entry[VALUE_SIZE_OFFSET:])
			if valueSize != uint32(len(tt.value)) {
				t.Errorf("valueSize = %d, want %d", valueSize, len(tt.value))
			}

			keyStart := KEY_OFFSET
			keyEnd := keyStart + len(tt.key)
			if !bytes.Equal(entry[keyStart:keyEnd], tt.key) {
				t.Error("key content mismatch")
			}

			valueStart := HEADER_SIZE + len(tt.key)
			valueEnd := valueStart + len(tt.value)
			if !bytes.Equal(entry[valueStart:valueEnd], tt.value) {
				t.Error("value content mismatch")
			}
		})
	}
}

func TestInitEntry_MaxSize(t *testing.T) {

	largeKey := make([]byte, 1024*1024)
	largeValue := make([]byte, 10*1024*1024)

	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entry := InitEntry(largeKey, largeValue, uint64(time.Now().Unix()))

	expectedSize := HEADER_SIZE + len(largeKey) + len(largeValue)
	if len(entry) != expectedSize {
		t.Errorf("entry size = %d, want %d", len(entry), expectedSize)
	}

	storedCRC := binary.LittleEndian.Uint32(entry[CRC_OFFSET:])
	calculatedCRC := crc32.ChecksumIEEE(entry[4:])
	if storedCRC != calculatedCRC {
		t.Error("CRC failed for large entry")
	}

}

func TestEntryParsing(t *testing.T) {
	tests := []struct {
		key      string
		value    string
		wantSize int
	}{
		{
			key:      "",
			value:    "",
			wantSize: 20,
		},
		{
			key:      "a",
			value:    "b",
			wantSize: 22,
		},
		{
			key:      "user:123",
			value:    "John Doe",
			wantSize: 36,
		},
		{
			key:      "key",
			value:    string(make([]byte, 1024)),
			wantSize: 1047,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s:%s", tt.key, tt.value[:min(len(tt.value), 10)]), func(t *testing.T) {
			timeStamp := uint64(time.Now().Unix())
			entry := InitEntry([]byte(tt.key), []byte(tt.value), uint64(timeStamp))

			if len(entry) != tt.wantSize {
				t.Errorf("entry size = %d, want %d", len(entry), tt.wantSize)
			}

			extractedKey, err := ExtractKey(entry)
			if err != nil {
				t.Fatal(err)
			}
			if string(extractedKey) != tt.key {
				t.Errorf("extracted key = %s, want %s", extractedKey, tt.key)
			}

			extractedValue, err := ExtractValue(entry)
			if err != nil {
				t.Fatal(err)
			}
			if string(extractedValue) != tt.value {
				t.Errorf("extracted value mismatch")
			}

			if err := VerifyEntryCRC(entry); err != nil {
				t.Errorf("CRC verification failed: %v", err)
			}
		})
	}
}

func TestPut(t *testing.T) {

	mf := MockFileSystem{}
	key, value := "key", "value"

	store, _ := Open("/dir/store", mf, true)

	timeStamp := uint64(time.Now().Unix())

	entry := InitEntry([]byte(key), []byte(value), timeStamp)

	if err := store.writeEntry(entry, key, []byte(value), timeStamp); err != nil {
		t.Errorf("Write failed %v", err)
	}

}
