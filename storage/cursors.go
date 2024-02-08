package storage

import "github.com/catlev/pkg/store/block"

type failingCursor struct {
	err error
}

// Err implements Cursor.
func (c *failingCursor) Err() error {
	return c.err
}

// Next implements Cursor.
func (failingCursor) Next() bool {
	return false
}

// This implements Cursor.
func (failingCursor) This() []block.Word {
	panic("unimplemented")
}
