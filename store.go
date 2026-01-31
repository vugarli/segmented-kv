package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"syscall"
	"time"
)

type FileSystem interface {
	Open(name string) (*os.File, error)
	CreateTemp(dir string, pattern string) (*os.File, error)
	Remove(name string) error
	ReadDir(name string) ([]fs.DirEntry, error)
}

type OSFileSystem struct{}

func (OSFileSystem) CreateTemp(dir string, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

func (OSFileSystem) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (OSFileSystem) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (OSFileSystem) Remove(name string) error {
	return os.Remove(name)
}

const (
	CRC_OFFSET       = 0
	TIMESTAMP_OFFSET = 4
	KEY_SIZE_OFFSET  = 12
	KEY_OFFSET       = 16
)

type LatestEntryRecord struct {
	FileId    uint32
	ValueSize uint32
	ValuePos  uint64
	Timestamp uint64
}

// 	Get(key string) ([]byte, error)
// 	ListKeys() ([]string, error)

// 	Put(key string, value []byte) error
// 	Delete(key string) error
// 	Merge(directoryName string) error
// 	Sync() error
// 	Close() error

type store struct {
	DirectoryName string
	KeyDir        map[string]LatestEntryRecord
}

type Store struct {
	*store
	syncOnPut bool
}

type ReadOnlyStore struct {
	*store
}

func (*store) Get(key string) ([]byte, error) {
	return nil, nil
}
func (*store) ListKeys() ([]string, error) {
	return nil, nil
}

func (*Store) Put(key string, value []byte) error {
	return nil
}

func (*Store) Delete(key string) error {
	return nil
}

func (*Store) Merge(directoryName string) error {
	return nil
}
func (*Store) Sync() error {
	return nil
}
func (*Store) Close() error {
	return nil
}

var (
	ErrInvalidStoreOperation          = errors.New("Invalid store operation error")
	ErrStoreDirectoryNotFound         = errors.New("Specified store directory doesn't exist error")
	ErrStoreDirectoryPermissionDenied = errors.New("Permission denied error")
)

func Open(directory string, fileSystem FileSystem, syncOnPut bool) (*Store, error) {
	if err := validateReadPermission(directory, fileSystem); err != nil {
		return nil, err
	}
	if err := validateWritePermission(directory, fileSystem); err != nil {
		return nil, err
	}

	// locking mechanisms?
	// check op then return appropriate store
	return &Store{
		store:     &store{DirectoryName: directory, KeyDir: make(map[string]LatestEntryRecord)},
		syncOnPut: syncOnPut}, nil
}

func OpenReadOnly(directory string, fileSystem FileSystem) (*ReadOnlyStore, error) {
	if err := validateReadPermission(directory, fileSystem); err != nil {
		return nil, err
	}
	return &ReadOnlyStore{
		store: &store{DirectoryName: directory, KeyDir: make(map[string]LatestEntryRecord)}}, nil
}

func validateReadPermission(directory string, fileSystem FileSystem) error {
	_, err := fileSystem.ReadDir(directory)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, syscall.ENOTDIR):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryPermissionDenied)
		default:
			return fmt.Errorf("cannot read directory %s: %w", directory, err)
		}
	}
	return nil
}

func validateWritePermission(directory string, fileSystem FileSystem) error {
	tmpFile, err := fileSystem.CreateTemp(directory, ".storecheck-*")
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, syscall.ENOTDIR):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryNotFound)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("%s: %w", directory, ErrStoreDirectoryPermissionDenied)
		default:
			return fmt.Errorf("cannot write to directory %s: %w", directory, err)
		}
	}
	if tmpFile != nil {
		tmpFile.Close()
		fileSystem.Remove(tmpFile.Name())
	}
	return nil
}

func InitEntry(key, value []byte) []byte {
	totalSize := 16 + len(key) + 4 + len(value)
	buf := make([]byte, totalSize)

	valueSizeOffset := KEY_OFFSET + len(key)
	valueOffset := valueSizeOffset + 4

	binary.LittleEndian.PutUint64(buf[TIMESTAMP_OFFSET:], uint64(time.Now().Unix()))
	binary.LittleEndian.PutUint32(buf[KEY_SIZE_OFFSET:], uint32(len(key)))
	copy(buf[KEY_OFFSET:], key)
	binary.LittleEndian.PutUint32(buf[valueSizeOffset:], uint32(len(value)))
	copy(buf[valueOffset:], value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[CRC_OFFSET:], crc)

	return buf
}

// func (s Store) Get(key string) []byte {

// }

// func (s Store) Set(key string, value []byte) {

// }
