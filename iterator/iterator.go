package iterator

import "github.com/hardcore-os/corekv/utils/codec"

// 迭代器
// 主要有以下几种继承
// 1、内存中的迭代器
// 2、磁盘中的存储
// 3、cache中的存储
// 4、vlog中的存储
type Iterator interface {
	Next()
	Valid() bool
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
