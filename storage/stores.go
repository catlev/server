package storage

import (
	"errors"

	"github.com/catlev/pkg/domain"
	"github.com/catlev/pkg/store/file"
)

var ErrReadOnly = errors.New("read only store")

type readOnlyStore struct {
	file *file.File
}

// AddBlock implements domain.Store.
func (*readOnlyStore) AddBlock(b *domain.Block) (domain.Word, error) {
	return 0, ErrReadOnly
}

// FreeBlock implements domain.Store.
func (*readOnlyStore) FreeBlock(id domain.Word) error {
	return ErrReadOnly
}

// ReadBlock implements domain.Store.
func (s *readOnlyStore) ReadBlock(id domain.Word, b *domain.Block) error {
	_, err := s.file.ReadAt(b.Bytes(), int64(id))
	return err
}

// WriteBlock implements domain.Store.
func (*readOnlyStore) WriteBlock(id domain.Word, b *domain.Block) (domain.Word, error) {
	return 0, ErrReadOnly
}

type transactionStore struct {
	tx      *file.Tx
	next    domain.Word
	updated map[domain.Word]domain.Block
	old     domain.Store
}

// AddBlock implements domain.Store.
func (s *transactionStore) AddBlock(b *domain.Block) (domain.Word, error) {
	next := s.next
	s.next += domain.ByteSize

	_, err := s.tx.WriteAt(b.Bytes(), int64(next))
	if err != nil {
		return 0, err
	}

	s.updated[next] = *b

	return next, nil
}

// FreeBlock implements domain.Store.
func (s *transactionStore) FreeBlock(id domain.Word) error {
	// just keep on adding blocks, whatever
	return nil
}

// ReadBlock implements domain.Store.
func (s *transactionStore) ReadBlock(id domain.Word, b *domain.Block) error {
	if updated, ok := s.updated[id]; ok {
		*b = updated
		return nil
	}

	return s.old.ReadBlock(id, b)
}

// WriteBlock implements domain.Store.
func (s *transactionStore) WriteBlock(id domain.Word, b *domain.Block) (domain.Word, error) {
	_, err := s.tx.WriteAt(b.Bytes(), int64(id))
	if err != nil {
		return 0, err
	}

	s.updated[id] = *b

	return id, nil
}
