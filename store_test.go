package main

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"
)

type MockFileInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	isDir bool
}

func (m MockFileInfo) Name() string       { return m.name }
func (m MockFileInfo) Size() int64        { return m.size }
func (m MockFileInfo) Mode() fs.FileMode  { return m.mode }
func (m MockFileInfo) ModTime() time.Time { return time.Now() }
func (m MockFileInfo) IsDir() bool        { return m.isDir }
func (m MockFileInfo) Sys() interface{}   { return nil }

type MockFileSystem struct {
	StatFunc       func(name string) (fs.FileInfo, error)
	CreateTempFunc func(dir, pattern string) (*os.File, error)
	RemoveFunc     func(name string) error
	ReadDirFunc    func(name string) ([]fs.DirEntry, error)
}

func (m MockFileSystem) Open(name string) (fs.File, error) {
	return nil, fs.ErrNotExist
}

func (m MockFileSystem) Stat(name string) (fs.FileInfo, error) {
	if m.StatFunc != nil {
		return m.StatFunc(name)
	}
	return nil, fs.ErrNotExist
}

func (m MockFileSystem) CreateTemp(dir string, pattern string) (*os.File, error) {
	if m.CreateTempFunc != nil {
		return m.CreateTempFunc(dir, pattern)
	}
	return nil, nil
}

func (m MockFileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	if m.ReadDirFunc != nil {
		return m.ReadDirFunc(name)
	}
	return nil, nil
}

func (m MockFileSystem) Remove(name string) error {
	if m.RemoveFunc != nil {
		return m.RemoveFunc(name)
	}
	return nil
}

func TestOpen(t *testing.T) {
	// invalid operation
	// directory doesn't exist
	// path is not directory
	// permission

	tests := []struct {
		name          string
		operation     Operation
		path          string
		expectedError error
		setupMock     func(*MockFileSystem)
	}{
		{
			name:          "Invalid operation",
			operation:     Operation(999),
			path:          "/valid/dir",
			expectedError: ErrInvalidStoreOperation,
			setupMock:     func(m *MockFileSystem) {},
		},
		{
			name:          "Directory doesn't exist - ReadWrite",
			operation:     ReadWrite,
			path:          "/nonexistent",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Directory doesn't exist - Read",
			operation:     Read,
			path:          "/nonexistent",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrNotExist
				}
			},
		},
		{
			name:          "Path is file not directory - ReadWrite",
			operation:     ReadWrite,
			path:          "/file.txt",
			expectedError: ErrStoreDirectoryNotFound,
			setupMock: func(m *MockFileSystem) {
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, syscall.ENOTDIR
				}
			},
		},
		{
			name:          "Path is file not directory - Read",
			operation:     Read,
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
			operation:     Read,
			path:          "/restricted",
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "No write permission",
			operation:     ReadWrite,
			path:          "/readonly",
			expectedError: ErrStoreDirectoryPermissionDenied,
			setupMock: func(m *MockFileSystem) {
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, os.ErrPermission
				}
			},
		},
		{
			name:          "Successful ReadWrite open",
			operation:     ReadWrite,
			path:          "/valid/store",
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.CreateTempFunc = func(dir, pattern string) (*os.File, error) {
					return nil, nil
				}
				m.RemoveFunc = func(name string) error {
					return nil
				}
			},
		},
		{
			name:          "Successful Read open",
			operation:     Read,
			path:          "/valid/store",
			expectedError: nil,
			setupMock: func(m *MockFileSystem) {
				m.ReadDirFunc = func(name string) ([]fs.DirEntry, error) {
					return []fs.DirEntry{}, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFS := &MockFileSystem{}
			tt.setupMock(mockFS)

			store, err := Open(tt.path, tt.operation, mockFS)

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
