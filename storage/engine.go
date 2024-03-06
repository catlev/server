package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/catlev/pkg/domain"
	"github.com/catlev/pkg/store/file"
	"github.com/catlev/pkg/store/tree"
)

var ErrUnknownTable = errors.New("unknown table")

type Engine struct {
	lock  sync.RWMutex
	f     *file.File
	depth int
	root  domain.Word
	store domain.Store
}

type Transaction struct {
	lock  sync.Mutex
	e     *Engine
	store *transactionStore
}

type Cursor interface {
	Next() bool
	This() []domain.Word
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

func (e *Engine) GetEntities(typeID domain.Word, key []domain.Word) Cursor {
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
			next:    domain.Word(stat.Size()),
			updated: map[domain.Word]domain.Block{},
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

func (tx *Transaction) GetEntities(typeID domain.Word, key []domain.Word) Cursor {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	return getEntities(tx, typeID, key)
}

func (tx *Transaction) PutEntity(typeID domain.Word, row []domain.Word) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	meta, err := queryMetadata(tx, typeID)
	if err != nil {
		return err
	}

	if meta.emptyTable() {
		err = addFirstTableBlock(tx, &meta)
		if err != nil {
			return err
		}
	}

	table := dataTree(tx, meta)
	return table.Put(row)
}

func (tx *Transaction) DeleteEntity(typeID domain.Word, key []domain.Word) error {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	meta, err := queryMetadata(tx, typeID)
	if err != nil {
		return err
	}

	if meta.emptyTable() {
		return nil
	}

	table := dataTree(tx, meta)
	return table.Delete(key)
}

type header struct {
	version domain.Word
	depth   int
	root    domain.Word
}

func getHeader(store domain.Store) (header, error) {
	var b domain.Block
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

type metaRow struct {
	id            domain.Word
	cols, keyCols int
	depth         int
	root          domain.Word
}

const (
	metaWidth = 5
	metaKey   = 1
)

func (r metaRow) emptyTable() bool {
	return r.root == 0
}

type metadataSource interface {
	metadataTree() *tree.Tree
	dataStore() domain.Store
}

func (e *Engine) dataStore() domain.Store {
	return e.store
}

func (e *Engine) metadataTree() *tree.Tree {
	return tree.New(metaWidth, metaKey, e.store, e.depth, e.root)
}

func (tx *Transaction) dataStore() domain.Store {
	return tx.store
}

func (tx *Transaction) metadataTree() *tree.Tree {
	return tree.New(metaWidth, metaKey, tx.store, tx.e.depth, tx.e.root)
}

func getEntities(source metadataSource, typeID domain.Word, key []domain.Word) Cursor {
	meta, err := queryMetadata(source, typeID)
	if err != nil {
		return &failingCursor{err: err}
	}

	if meta.emptyTable() {
		return &failingCursor{}
	}

	table := dataTree(source, meta)
	return table.GetRange(key)
}

func queryMetadata(source metadataSource, typeID domain.Word) (metaRow, error) {
	metaT := source.metadataTree()
	meta, err := metaT.Get([]domain.Word{typeID})
	if errors.Is(err, tree.ErrNotFound) {
		return metaRow{}, fmt.Errorf("%w with id %x", ErrUnknownTable, typeID)
	}
	if err != nil {
		return metaRow{}, err
	}

	return metaRow{
		id:      meta[0],
		cols:    int(meta[1]),
		keyCols: int(meta[2]),
		depth:   int(meta[3]),
		root:    meta[4],
	}, nil
}

func dataTree(source metadataSource, meta metaRow) *tree.Tree {
	return tree.New(meta.cols, meta.keyCols, source.dataStore(), meta.depth, meta.root)
}

func getTable(source metadataSource, typeID domain.Word) (*tree.Tree, error) {
	meta, err := queryMetadata(source, typeID)
	if err != nil {
		return nil, err
	}
	return dataTree(source, meta), nil
}

func addFirstTableBlock(source metadataSource, meta *metaRow) error {
	id, err := source.dataStore().AddBlock(&domain.Block{})
	if err != nil {
		return err
	}

	meta.root = id
	return source.metadataTree().Put([]domain.Word{
		meta.id,
		domain.Word(meta.cols),
		domain.Word(meta.keyCols),
		domain.Word(meta.depth),
		meta.root,
	})
}
