package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type GroupSavingStrat func(group []MergeEntryRecord) ([]MergeResult, error)
type GroupCorruptionStrat func(results []MergeResult)
type GroupOnSuccessStrat func(results []MergeResult)

func GenerateHintDecorator(fileSystem FileSystem, directory string, encodeHintEntries func(*os.File, []MergeResult) error, onSuccess GroupOnSuccessStrat) GroupOnSuccessStrat {
	return func(results []MergeResult) {
		onSuccess(results)
		// generate Hints
		groups := groupMergeResultsByFileId(results)

		for fileId, group := range groups {
			// generate hint file and save
			hintFileTempPath := filepath.Join(directory, fmt.Sprintf("%d.hinttemp", fileId))
			hintFile, err := os.OpenFile(hintFileTempPath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				// log
				continue
			}
			if err := encodeHintEntries(hintFile, group); err != nil {
				hintFile.Close()
				os.Remove(hintFileTempPath)
				// log
			}
			if err := hintFile.Sync(); err != nil {
				hintFile.Close()
				os.Remove(hintFileTempPath)
				//log
			}
			hintFile.Close()
			hintFileFinalPath := filepath.Join(directory, fmt.Sprintf("%d.hint", fileId))

			if err := os.Rename(hintFileTempPath, hintFileFinalPath); err != nil {
				os.Remove(hintFileTempPath)
				// log commit failed
			}
		}
	}
}

func writeHintEntriesToDisk(file *os.File, results []MergeResult) error {
	var offset int64
	for _, result := range results {
		n, err := writeHintEntry(file, result, offset)
		if err != nil {
			return fmt.Errorf("Failed to write hint entries to file: %w", err)
		}
		offset += n
	}
	return nil
}

func writeHintEntry(file *os.File, resultEntry MergeResult, offset int64) (int64, error) {
	entry := InitHintEntry(resultEntry)
	n, err := file.WriteAt(entry, offset)

	if n != len(entry) || err != nil {
		return 0, fmt.Errorf("Writing hint file error")
	}

	return int64(len(entry)), nil
}

type HintEntryHeader struct {
	Timestamp uint64
	KeySize   uint32
	ValueSize uint32
	ValuePos  uint64
}

const (
	HINT_HEADER_SIZE = 24

	HINT_TSTAMP_OFFSET = 0
	HINT_KEYSZ_OFFSET  = 8
	HINT_VALSZ_OFFSET  = 12
	HINT_VALPOS_OFFSET = 16
)

func readHintEntry(file *os.File, offset int64) (*HintEntryHeader, string, error) {
	hintEntrySize := TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + VALUE_POS_SIZE
	buf := make([]byte, hintEntrySize)

	n, err := file.ReadAt(buf, offset)
	if err == io.EOF {
		return nil, "", err
	}
	if err != nil {
		return nil, "", fmt.Errorf("Read: %d/%d bytes. Reading hint header at offset %d: %w", n, HINT_HEADER_SIZE, offset, err)
	}
	hintEntryHeader := parseHintEntryHeader(buf)

	keyBuf := make([]byte, hintEntryHeader.KeySize)

	offset += int64(n)
	n, err = file.ReadAt(keyBuf, offset)
	if err == io.EOF {
		return nil, "", err
	}
	if err != nil {
		return nil, "", fmt.Errorf("Read: %d/%d bytes. Reading hint key at offset %d: %w", n, hintEntryHeader.KeySize, offset, err)
	}

	return &hintEntryHeader, string(keyBuf), nil
}

func parseHintEntryHeader(buf []byte) HintEntryHeader {
	timestamp := binary.LittleEndian.Uint64(buf[HINT_TSTAMP_OFFSET:])
	keySize := binary.LittleEndian.Uint32(buf[HINT_KEYSZ_OFFSET:])
	valSize := binary.LittleEndian.Uint32(buf[HINT_VALSZ_OFFSET:])
	valPos := binary.LittleEndian.Uint64(buf[HINT_VALPOS_OFFSET:])

	return HintEntryHeader{
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valSize,
		ValuePos:  valPos,
	}
}

func InitHintEntry(resultEntry MergeResult) []byte {
	// TSTAMP KSZ VSZ VPOS KEY
	entrySize := TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + VALUE_POS_SIZE + len(resultEntry.Key)
	buf := make([]byte, entrySize)

	binary.LittleEndian.PutUint64(buf[0:], resultEntry.StaleEntry.Timestamp)
	binary.LittleEndian.PutUint32(buf[0+TSTAMP_SIZE:], uint32(len(resultEntry.Key)))
	binary.LittleEndian.PutUint32(buf[0+TSTAMP_SIZE+KEY_SIZE_SIZE:], resultEntry.StaleEntry.ValueSize)
	binary.LittleEndian.PutUint64(buf[0+TSTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE:], resultEntry.ValuePos)
	copy(buf[0+TSTAMP_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+VALUE_POS_SIZE:], []byte(resultEntry.Key))

	return buf
}

func groupMergeResultsByFileId(results []MergeResult) map[int][]MergeResult {
	groupedEntries := make(map[int][]MergeResult)
	for _, result := range results {
		groupedEntries[result.FileId] = append(groupedEntries[result.FileId], result)
	}
	return groupedEntries
}

// TODO filesystem rename
func CommitToDisk(directory string, fileSystem FileSystem) GroupOnSuccessStrat {
	return func(results []MergeResult) {
		resultedFileIds := make(map[int]struct{})

		for _, result := range results {
			if _, exists := resultedFileIds[result.FileId]; !exists {
				resultedFileIds[result.FileId] = struct{}{}

				oldPath := filepath.Join(directory, fmt.Sprintf("%d.datatemp", result.FileId))
				newPath := filepath.Join(directory, fmt.Sprintf("%d.data", result.FileId))
				os.Rename(oldPath, newPath)
				//log error
			}
		}

	}
}

func CleanCorruptedFromDisk(directory string, fileSystem FileSystem) GroupCorruptionStrat {
	return func(results []MergeResult) {
		for _, result := range results {
			filePath := filepath.Join(directory, fmt.Sprintf("%d.data", result.FileId))
			if err := fileSystem.Remove(filePath); err != nil {
				//log
			}
		}
	}
}

// TODO add fileSystem contract
func SaveToDisk(directory string, idFetcher func() (int, bool)) GroupSavingStrat {

	return func(group []MergeEntryRecord) ([]MergeResult, error) {
		fileId, ok := idFetcher()
		if !ok {
			return nil, fmt.Errorf("id sequence exhausted")
		}

		destinationFileName := filepath.Join(directory, fmt.Sprintf("%d.datatemp", fileId))
		destinationFile, err := os.OpenFile(destinationFileName, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("opening destination file: %w", err)
		}
		defer destinationFile.Close()

		result := make([]MergeResult, 0, len(group))
		var currentOffset uint64
		for _, staleEntry := range group {
			originFile, err := os.Open(filepath.Join(directory, fmt.Sprintf("%d.data", staleEntry.Record.FileId)))
			if err != nil {
				return nil, fmt.Errorf("%w Opening origin file: %w", ErrCorruptedState, err)
			}
			entry, entryHeader, err := readEntry(originFile, uint64(staleEntry.Record.ValuePos-HEADER_SIZE-uint64(len(staleEntry.Key))))
			originFile.Close()
			if err != nil {
				return result, fmt.Errorf("%w Reading entry: %w", ErrCorruptedState, err)
			}
			n, err := destinationFile.WriteAt(entry, int64(currentOffset))
			if err != nil {
				return result, fmt.Errorf("%w Writing entry: %w", ErrCorruptedState, err)
			}
			if n != len(entry) {
				return result, fmt.Errorf("%w Incomplete write: wrote %d of %d bytes", ErrCorruptedState, n, len(entry))
			}
			newValuePos := currentOffset + HEADER_SIZE + uint64(entryHeader.KeySize)
			result = append(result, MergeResult{
				StaleEntry: staleEntry.Record,
				Key:        staleEntry.Key,
				ValuePos:   newValuePos,
				FileId:     fileId,
			})
			currentOffset += uint64(len(entry))
		}
		if err := destinationFile.Sync(); err != nil {
			return nil, fmt.Errorf("syncing destination file: %w", err)
		}
		return result, nil
	}
}
