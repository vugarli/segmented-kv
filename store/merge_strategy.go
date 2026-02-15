package store

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"time"
)

type Mergable interface {
	Merge(...MergeEntryRecordsFilter) error
}

type MergeScheduler func(Mergable, ...MergeEntryRecordsFilter)

func (store *RWStore) ScheduleMerge(scheduler MergeScheduler, filters ...MergeEntryRecordsFilter) {
	scheduler(store, filters...)
}

func PeriodicMerge(ctx context.Context, duration time.Duration) MergeScheduler {
	return func(store Mergable, filters ...MergeEntryRecordsFilter) {
		go func() {
			ticker := time.NewTicker(duration)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					store.Merge(filters...)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

type MergeEntryRecordsFilter func(map[int][]MergeEntryRecord)

// Filters out entries which belong to a file which has less than minLiveDataRatio
// Ex: minLiveDataRation: 0.4 means files with 40% or more live data are exempt from merging.
func LiveRatioFilter(minLiveDataRatio float64, directory string) MergeEntryRecordsFilter {
	return func(mer map[int][]MergeEntryRecord) {

		group := make(map[int]float64) //fileId;ratio

		for fileId, entries := range maps.All(mer) {
			fs, err := os.Stat(filepath.Join(directory, fmt.Sprintf("%d.data", fileId)))
			if err != nil {
				continue //log
			}

			for _, entry := range entries {
				r := float64(entry.Record.EntrySize()) / float64(fs.Size())
				group[entry.Record.FileId] += r
			}
		}

		for k := range group {
			if group[k] >= minLiveDataRatio {
				delete(mer, k)
			}

		}
	}
}
