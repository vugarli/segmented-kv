package main

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"
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

// func TestInitEntry(t *testing.T) {
// 	key := []byte("mykey")
// 	value := []byte("myvalue")
// 	entrySize := 4 + 8 + 4 + len(key) + 4 + len(value)

// 	entry := InitEntry(key, value)

// 	if len(entry) != entrySize {
// 		t.Fatalf("Expected size of entry is %d, but got %d", entrySize, len(entry))
// 	}

// 	// CRC check
// 	gotCrc := binary.LittleEndian.Uint32(entry[0:4])
// 	calculatedCRC := crc32.ChecksumIEEE(entry[4:])
// 	if gotCrc != calculatedCRC {
// 		t.Fatalf("Wrong CRC!")
// 	}

// 	//key-value check
// 	gotKey := entry[KEY_OFFSET : KEY_OFFSET+len(key)]
// 	gotValue := entry[KEY_OFFSET+len(key)+4 : KEY_OFFSET+len(key)+4+len(value)]

// 	if string(gotValue) != string(value) {
// 		t.Fatalf("Expected the value of %s, but got %s", string(gotValue), string(value))
// 	}
// 	if string(gotKey) != string(key) {
// 		t.Fatalf("Expected the key of %s, but got %s", string(gotKey), string(key))
// 	}
// }
