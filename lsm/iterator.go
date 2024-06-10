package lsm

import (
	"corekv/iterator"
	"corekv/utils"
	"corekv/utils/codec"
)

type Iterator struct {
	item  iterator.Item
	iters []iterator.Iterator
}

func (i *Iterator) Next() {
	i.iters[0].Next()
}

func (i *Iterator) Valid() bool {
	return i.iters[0].Valid()
}

func (i *Iterator) Rewind() {
	i.iters[0].Rewind()
}

func (i *Iterator) Item() iterator.Item {
	return i.iters[0].Item()
}

func (i *Iterator) Close() error {
	return nil
}

type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}

func (lsm *LSM) NewIterator(opt *iterator.Options) iterator.Iterator {
	iter := &Iterator{}
	iter.iters = make([]iterator.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immuTable {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.NewIteraotr(opt))
	return iter
}

// 内存迭代器
type MemIterator struct {
	it    iterator.Item
	iters []*Iterator
	sl    *utils.SkipList
}

func (m *MemIterator) Next() {
	m.it = nil
}

func (m *MemIterator) Valid() bool {
	return m.it != nil
}

func (m *MemIterator) Rewind() {
	entry := m.sl.Search([]byte("hello"))
	m.it = &Item{e: entry}
}

func (m *MemIterator) Item() iterator.Item {
	return m.it
}

func (m *MemIterator) Close() error {
	return nil
}

func (m *memTable) NewIterator(opt *iterator.Options) iterator.Iterator {
	return &MemIterator{sl: m.sl}
}

// LevelIterator levelmanager迭代器
type LevelIterator struct {
	iter  *iterator.Item
	iters []*Iterator
}

func (l *LevelIterator) Next() {
}

func (l *LevelIterator) Valid() bool {
	return false
}

func (l *LevelIterator) Rewind() {

}

func (l *LevelIterator) Item() iterator.Item {
	return &Item{}
}

func (l *LevelIterator) Close() error {
	return nil
}

func (lm *levelManager) NewIteraotr(opt *iterator.Options) iterator.Iterator {
	return &LevelIterator{}
}
