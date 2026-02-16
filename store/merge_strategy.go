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
	Merge(MergeCandidateRetrievalStrat) error
}

type MergeScheduler func(Mergable, MergeCandidateRetrievalStrat)

// During merge, lock is held on store.
func (store *RWStore) ScheduleMerge(strat MergeCandidateRetrievalStrat, scheduler MergeScheduler) {
	scheduler(store, strat)
}

// store.ScheduleMerge(Strat(filters),PeriodicMerge(ctx,dur))

func PeriodicMerge(ctx context.Context, duration time.Duration) MergeScheduler {
	return func(store Mergable, strat MergeCandidateRetrievalStrat) {
		go func() {
			ticker := time.NewTicker(duration)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					store.Merge(strat)
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

func FilterCurrentFileOut(currentFileId int) MergeEntryRecordsFilter {
	return func(entries map[int][]MergeEntryRecord) {
		delete(entries, currentFileId)
	}
}

type MergeCandidateRetrievalStrat func(KeyDir) map[int][]MergeEntryRecord

func applyFilters(entries map[int][]MergeEntryRecord, filters ...MergeEntryRecordsFilter) map[int][]MergeEntryRecord {
	for _, filter := range filters {
		filter(entries)
	}
	return entries
}

func groupEntriesByFileId(keyDir KeyDir) map[int][]MergeEntryRecord {
	var entries map[int][]MergeEntryRecord = make(map[int][]MergeEntryRecord)
	for key, record := range maps.All(keyDir) {
		entries[record.FileId] = append(entries[record.FileId], MergeEntryRecord{
			Record: record,
			Key:    key,
		})
	}
	return entries
}
func FilteredMergeEntryRetrievalStrat(filters ...MergeEntryRecordsFilter) MergeCandidateRetrievalStrat {
	return func(kd KeyDir) map[int][]MergeEntryRecord {
		return applyFilters(groupEntriesByFileId(kd), filters...)
	}
}
