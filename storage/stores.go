package storage

import (
	"errors"

	"github.com/catlev/pkg/store/block"
	"github.com/catlev/pkg/store/file"
)

var ErrReadOnly = errors.New("read only store")

type readOnlyStore struct {
	file *file.File
}

// AddBlock implements block.Store.
func (*readOnlyStore) AddBlock(b *block.Block) (block.Word, error) {
	return 0, ErrReadOnly
}

// FreeBlock implements block.Store.
func (*readOnlyStore) FreeBlock(id block.Word) error {
	return ErrReadOnly
}

// ReadBlock implements block.Store.
func (s *readOnlyStore) ReadBlock(id block.Word, b *block.Block) error {
	_, err := s.file.ReadAt(b.Bytes(), int64(id))
	return err
}

// WriteBlock implements block.Store.
func (*readOnlyStore) WriteBlock(id block.Word, b *block.Block) (block.Word, error) {
	return 0, ErrReadOnly
}

type transactionStore struct {
	tx      *file.Tx
	next    block.Word
	updated map[block.Word]block.Block
	old     block.Store
}

// AddBlock implements block.Store.
func (s *transactionStore) AddBlock(b *block.Block) (block.Word, error) {
	next := s.next
	s.next += block.ByteSize

	_, err := s.tx.WriteAt(b.Bytes(), int64(next))
	if err != nil {
		return 0, err
	}

	s.updated[next] = *b

	return next, nil
}

// FreeBlock implements block.Store.
func (s *transactionStore) FreeBlock(id block.Word) error {
	// just keep on adding blocks, whatever
	return nil
}

// ReadBlock implements block.Store.
func (s *transactionStore) ReadBlock(id block.Word, b *block.Block) error {
	if updated, ok := s.updated[id]; ok {
		*b = updated
		return nil
	}

	return s.old.ReadBlock(id, b)
}

// WriteBlock implements block.Store.
func (s *transactionStore) WriteBlock(id block.Word, b *block.Block) (block.Word, error) {
	_, err := s.tx.WriteAt(b.Bytes(), int64(id))
	if err != nil {
		return 0, err
	}

	s.updated[id] = *b

	return id, nil
}
