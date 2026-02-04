package main

import (
	"io/fs"
	"os"
	"time"
)

const (
	MockLockExc = iota
	MockLockShr
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
	lockedPathes             map[string]int
	StatFunc                 func(name string) (fs.FileInfo, error)
	CreateTempFunc           func(dir, pattern string) (*os.File, error)
	CreateFunc               func(path string) (*os.File, error)
	RemoveFunc               func(name string) error
	ReadDirFunc              func(name string) ([]fs.DirEntry, error)
	acquireExclusiveLockFunc func(directory string) (*os.File, error)
	acquireSharedLockFunc    func(directory string) (*os.File, error)
	getNewFileIdFunc         func(directory string) (uint32, error)
}

func (m MockFileSystem) getNewFileId(directory string) (uint32, error) {
	if m.getNewFileIdFunc != nil {
		return m.getNewFileIdFunc(directory)
	}
	return 0, nil

}

func (m MockFileSystem) acquireExclusiveLock(directory string) (*os.File, error) {
	if m.acquireExclusiveLockFunc != nil {
		return m.acquireExclusiveLockFunc(directory)
	}
	return nil, nil
}

func (m MockFileSystem) acquireSharedLock(directory string) (*os.File, error) {
	if m.acquireSharedLockFunc != nil {
		return m.acquireSharedLockFunc(directory)
	}
	return nil, nil
}

func (m MockFileSystem) isEcxlusiveLocked(path string) bool {
	l, ok := m.lockedPathes[path]
	return ok && l == MockLockExc
}

func (m MockFileSystem) isSharedLocked(path string) bool {
	l, ok := m.lockedPathes[path]
	return ok && l == MockLockShr
}

func (m MockFileSystem) Open(name string) (*os.File, error) {
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
func (m MockFileSystem) Create(path string) (*os.File, error) {
	// if m.CreateTempFunc != nil {
	// 	return m.CreateFunc(path)
	// }
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
