package lsm

import "corekv/file"

/*
*
代表每个sstable
*/
type Table struct {
	ss *file.SSTable
}

func openTable(opt *Options) *Table {
	return &Table{ss: file.OpenSSTable(&file.Options{})}
}
