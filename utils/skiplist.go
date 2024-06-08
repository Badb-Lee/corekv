package utils

import "corekv/utils/codec"

/**
跳表，实现索引的数据结构
*/

type node struct {
	member *codec.Entry
	next   *node
	prev   *node
	levels []*node
}

type SkipList struct {
	maxLevel int
	head     *node
}

func (sl *SkipList) Close() error {
	return nil
}

// Search 查找
func (sl *SkipList) Search(key []byte) *codec.Entry {
	return sl.head.member
}

// Insert 插入
func (sl *SkipList) Insert(entry *codec.Entry) error {
	sl.head.next = &node{
		member: entry,
	}

	return nil
}

func NewSkipList() *SkipList {
	return &SkipList{
		maxLevel: 0,
		head: &node{
			member: nil,
			next:   nil,
			levels: make([]*node, 0, 64),
		},
	}
}
