package iterator

import "corekv/utils/codec"

/*
*
此文件是迭代器
主要有四个地方继承
1、内存
2、磁盘
3、vlog
*/
type Iterator interface {
	Next()
	Valid() bool
	// 从集合的第一个元素开始
	Rewind()
	Item() Item
	Close() error
}

type Item interface {
	Entry() *codec.Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
