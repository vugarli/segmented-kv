package store

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

func fold[T, U any](slice []T, initial U, f func(U, T) U) U {
	acc := initial
	for _, v := range slice {
		acc = f(acc, v)
	}
	return acc
}

// test coverage needed
type fileHandlerCache struct {
	mu    sync.Mutex
	cache map[int]*os.File
}

func (fh *fileHandlerCache) get(directory string, id int) (*os.File, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if file, exists := fh.cache[id]; exists {
		return file, nil
	}

	file, err := os.Open(filepath.Join(directory, fmt.Sprintf("%d.data", id)))
	if err != nil {
		return nil, err
	}
	fh.cache[id] = file
	return file, nil
}

func (fh *fileHandlerCache) evict(id int) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if f, exist := fh.cache[id]; exist {
		f.Close()
		delete(fh.cache, id)
	}
}

func (fh *fileHandlerCache) evictall() {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	for id, f := range maps.All(fh.cache) {
		f.Close()
		delete(fh.cache, id)
	}
}
