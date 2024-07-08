package lsm

import "corekv/file"

/*
*
代表每个sstable
*/
type Table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

//func openTable(opt *Options) *Table {
//	return &Table{ss: file.OpenSSTable(&file.Options{})}
//}

// Size is its file size in bytes
func (t *Table) Size() int64 { return int64(t.ss.Size()) }

// StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST.
func (t *Table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *Table {

}
