package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/catlev/pkg/store/block"
	"github.com/catlev/pkg/store/file"
	"github.com/catlev/pkg/store/tree"
)

var ErrUnknownTable = errors.New("unknown table")

type Engine struct {
	lock  sync.RWMutex
	f     *file.File
	depth int
	root  block.Word
	store block.Store
}

type Transaction struct {
	lock  sync.Mutex
	e     *Engine
	store *transactionStore
}

type Cursor interface {
	Next() bool
	This() []block.Word
	Err() error
}

func New(path string) (*Engine, error) {
	f, err := file.Open(path)
	if err != nil {
		return nil, err
	}

	store := &readOnlyStore{file: f}
	header, err := getHeader(store)
	if err != nil {
		return nil, err
	}

	return &Engine{
		f:     f,
		depth: header.depth,
		root:  header.root,
		store: store,
	}, nil
}

func (e *Engine) Close() error {
	return e.f.Close()
}

func (e *Engine) GetEntities(typeID block.Word, key []block.Word) Cursor {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return getEntities(e, typeID, key)
}

func (e *Engine) Begin() (*Transaction, error) {
	e.lock.Lock()

	tx, err := e.f.Begin()
	if err != nil {
		return nil, err
	}

	stat, err := e.f.Stat()
	if err != nil {
		return nil, err
	}

	return &Transaction{
		e: e,
		store: &transactionStore{
			tx:      tx,
			next:    block.Word(stat.Size()),
			updated: map[block.Word]block.Block{},
			old:     e.store,
		},
	}, nil
}

func (tx *Transaction) Commit() error {
	defer tx.e.lock.Unlock()

	return tx.store.tx.Commit()
}

func (tx *Transaction) Rollback() error {
	defer tx.e.lock.Unlock()

	return nil
}

func (tx *Transaction) GetEntities(typeID block.Word, key []block.Word) Cursor {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	return getEntities(tx, typeID, key)
}

func (tx *Transaction) PutEntity(typeID block.Word, row []block.Word) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	meta, err := queryMetadata(tx, typeID)
	if err != nil {
		return err
	}

	if meta[tableRootCol] == 0 {
		err = addFirstTableBlock(tx, meta)
		if err != nil {
			return err
		}
	}

	table := dataTree(tx, meta)
	return table.Put(row)
}

func (tx *Transaction) DeleteEntity(typeID block.Word, key []block.Word) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	meta, err := queryMetadata(tx, typeID)
	if err != nil {
		return err
	}

	if meta[tableRootCol] == 0 {
		return nil
	}

	table := dataTree(tx, meta)
	return table.Delete(key)
}

type header struct {
	version block.Word
	depth   int
	root    block.Word
}

func getHeader(store block.Store) (header, error) {
	var b block.Block
	err := store.ReadBlock(0, &b)
	if err != nil {
		return header{}, err
	}

	return header{
		version: b[0],
		depth:   int(b[1]),
		root:    b[2],
	}, nil
}

const (
	tableIDCol = iota
	tableColsCol
	tableKeyCol
	tableDepthCol
	tableRootCol
	tableColumnCount
)

type metadataSource interface {
	metadataTree() *tree.Tree
	dataStore() block.Store
}

func getEntities(source metadataSource, typeID block.Word, key []block.Word) Cursor {
	meta, err := queryMetadata(source, typeID)
	if err != nil {
		return &failingCursor{err: err}
	}

	if meta[tableRootCol] == 0 {
		return &failingCursor{}
	}

	table := dataTree(source, meta)
	return table.GetRange(key)
}

func queryMetadata(source metadataSource, typeID block.Word) ([]block.Word, error) {
	metaT := source.metadataTree()
	meta, err := metaT.Get([]block.Word{typeID})
	if errors.Is(err, tree.ErrNotFound) {
		return nil, fmt.Errorf("%w with id %x", ErrUnknownTable, typeID)
	}
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func dataTree(source metadataSource, meta []block.Word) *tree.Tree {
	keyCols := make([]int, meta[tableKeyCol])
	for i := range keyCols {
		keyCols[i] = i
	}

	cols := int(meta[tableColsCol])
	depth := int(meta[tableDepthCol])
	root := meta[tableRootCol]
	return tree.New(cols, keyCols, source.dataStore(), depth, root)
}

func (e *Engine) dataStore() block.Store {
	return e.store
}

func (e *Engine) metadataTree() *tree.Tree {
	return tree.New(tableColumnCount, []int{tableIDCol}, e.store, e.depth, e.root)
}

func (tx *Transaction) dataStore() block.Store {
	return tx.store
}

func (tx *Transaction) metadataTree() *tree.Tree {
	return tree.New(tableColumnCount, []int{tableIDCol}, tx.store, tx.e.depth, tx.e.root)
}

func addFirstTableBlock(source metadataSource, meta []block.Word) error {
	id, err := source.dataStore().AddBlock(&block.Block{})
	if err != nil {
		return err
	}

	meta[tableRootCol] = id
	return source.metadataTree().Put(meta)
}
