package storage

import (
	"bytes"
	"errors"
	"slices"
	"unsafe"

	"github.com/catlev/pkg/domain"
	"github.com/catlev/pkg/store/tree"
)

const (
	stringTypeID       = 1
	stringTypeLookupID = 2
	counterTypeID      = 3
	stringCounterID    = 1
)

func nextCounter(tx *Transaction, id domain.Word) (domain.Word, error) {
	data, err := getTable(tx, counterTypeID)
	if err != nil {
		return 0, err
	}

	row, err := data.Get([]domain.Word{id})
	if err != nil {
		return 0, err
	}
	current := row[1]

	err = tx.PutEntity(stringTypeLookupID, []domain.Word{id, current + 1})
	if err != nil {
		return 0, err
	}

	return current, nil
}

func retrieveString(source metadataSource, value domain.Word) (string, error) {
	data, err := getTable(source, stringTypeID)
	if err != nil {
		return "", err
	}

	var parts []domain.Word
	key := value
	for key != 0 {
		row, err := data.Get([]domain.Word{key})
		if err != nil {
			return "", err
		}
		parts = append(parts, row[2])
		key = row[1]
	}

	slices.Reverse(parts)
	return string(unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(parts))), len(parts)*8)), nil
}

func retrieveStringValue(source metadataSource, str string) (domain.Word, bool, error) {
	data, err := getTable(source, stringTypeLookupID)
	if err != nil {
		return 0, false, err
	}

	parts := make([]domain.Word, (len(str)+7)/8)
	copy(wordsToBytes(parts), []byte(str))

	var key domain.Word
	for _, p := range parts {
		row, err := data.Get([]domain.Word{key, p})
		if errors.Is(err, tree.ErrNotFound) {
			return 0, false, nil
		}
		key = row[2]
	}
	return key, true, nil
}

func ensureStringValue(tx *Transaction, str string) (domain.Word, error) {
	data, err := getTable(tx, stringTypeLookupID)
	if err != nil {
		return 0, err
	}

	parts := make([]domain.Word, (len(str)+7)/8)
	copy(wordsToBytes(parts), []byte(str))

	var key domain.Word
	for _, p := range parts {
		row, err := data.Get([]domain.Word{key, p})
		if errors.Is(err, tree.ErrNotFound) {
			next, err := nextCounter(tx, stringCounterID)
			if err != nil {
				return 0, err
			}
			err = tx.PutEntity(stringTypeID, []domain.Word{next, p, key})
			if err != nil {
				return 0, err
			}
			err = tx.PutEntity(stringTypeLookupID, []domain.Word{key, p, next})
			if err != nil {
				return 0, err
			}
			key = next
			continue
		}
		if err != nil {
			return 0, err
		}
		key = row[2]
	}

	return key, nil
}

func wordsToBytes(parts []domain.Word) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(parts))), len(parts)*8)
}

func partsToString(parts []domain.Word) string {
	buf := bytes.TrimRight(wordsToBytes(parts), "\000")
	return string(buf)
}
