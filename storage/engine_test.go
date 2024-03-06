package storage

import (
	"io"
	"os"
	"testing"

	"github.com/catlev/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadEmptyDB(t *testing.T) {
	e, err := New("./testdata/empty.db")
	require.NoError(t, err)
	defer e.Close()

	assert.NotNil(t, e)

	c := e.GetEntities(0, nil)
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{0, 6, 1, 0, 512, 0}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())
}

func TestReadEmptyTable(t *testing.T) {
	e, err := New("./testdata/empty-table.db")
	require.NoError(t, err)
	defer e.Close()

	assert.NotNil(t, e)

	c := e.GetEntities(1, nil)

	assert.False(t, c.Next())
	assert.NoError(t, c.Err())
}

func TestReadTable(t *testing.T) {
	e, err := New("./testdata/small-table.db")
	require.NoError(t, err)
	defer e.Close()

	assert.NotNil(t, e)

	c := e.GetEntities(1, []domain.Word{5})

	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{5, 69}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())
}

func TestPutTable(t *testing.T) {
	from, err := os.Open("./testdata/small-table.db")
	require.NoError(t, err)

	to, err := os.CreateTemp("", "")
	name := to.Name()
	require.NoError(t, err)
	defer os.Remove(name)

	_, err = io.Copy(to, from)
	require.NoError(t, err)

	from.Close()
	to.Close()

	e, err := New(name)
	require.NoError(t, err)

	c := e.GetEntities(1, []domain.Word{13})
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{5, 69}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	tx, err := e.Begin()
	require.NoError(t, err)

	tx.PutEntity(1, []domain.Word{13, 45})

	c = tx.GetEntities(1, []domain.Word{13})
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{13, 45}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	require.NoError(t, tx.Commit())

	c = e.GetEntities(1, []domain.Word{13})
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{13, 45}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

}

func TestPutEmptyTable(t *testing.T) {
	from, err := os.Open("./testdata/empty-table.db")
	require.NoError(t, err)

	to, err := os.CreateTemp("", "")
	name := to.Name()
	require.NoError(t, err)
	defer os.Remove(name)

	_, err = io.Copy(to, from)
	require.NoError(t, err)

	from.Close()
	to.Close()

	e, err := New(name)
	require.NoError(t, err)

	c := e.GetEntities(1, []domain.Word{13})
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	tx, err := e.Begin()
	require.NoError(t, err)

	tx.PutEntity(1, []domain.Word{13, 45})

	c = tx.GetEntities(1, []domain.Word{13})
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{13, 45}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	require.NoError(t, tx.Commit())

	c = e.GetEntities(1, []domain.Word{13})
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{13, 45}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

}

func TestDeleteTable(t *testing.T) {
	from, err := os.Open("./testdata/small-table.db")
	require.NoError(t, err)

	to, err := os.CreateTemp("", "")
	name := to.Name()
	require.NoError(t, err)
	defer os.Remove(name)

	_, err = io.Copy(to, from)
	require.NoError(t, err)

	from.Close()
	to.Close()

	e, err := New(name)
	require.NoError(t, err)

	c := e.GetEntities(1, nil)
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{0, 160}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{1, 6}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{5, 69}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	tx, err := e.Begin()
	require.NoError(t, err)

	tx.DeleteEntity(1, []domain.Word{5})

	c = tx.GetEntities(1, nil)
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{0, 160}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{1, 6}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

	require.NoError(t, tx.Commit())

	c = e.GetEntities(1, nil)
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{0, 160}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{1, 6}, c.This())
	assert.True(t, c.Next())
	assert.Equal(t, []domain.Word{16, 21845}, c.This())
	assert.False(t, c.Next())
	assert.NoError(t, c.Err())

}
