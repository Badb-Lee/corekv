package corekv

import (
	"corekv/iterator"
	"corekv/utils/codec"
)

type DBIterator struct {
	iters []iterator.Iterator
}

func (D *DBIterator) Next() {
	D.iters[0].Next()
}

func (D *DBIterator) Valid() bool {
	return D.iters[0].Valid()
}

func (D *DBIterator) Rewind() {
	D.iters[0].Rewind()
}

func (D *DBIterator) Item() iterator.Item {
	return D.iters[0].Item()
}

func (D *DBIterator) Close() error {
	return nil
}

type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry { return it.e }

func (db *DB) NewIterator(opt *iterator.Options) iterator.Iterator {
	dbIterator := &DBIterator{}
	dbIterator.iters = make([]iterator.Iterator, 0)
	dbIterator.iters = append(dbIterator.iters, db.lsm.NewIterator(opt))
	return dbIterator
}
