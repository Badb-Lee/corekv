package lsm

import (
	"corekv/file"
	"corekv/utils"
	"github.com/pkg/errors"
	"os"
	"sync/atomic"
)

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
	sstSize := int(lm.opt.SSTableMaxSz)
	// 如果不为空，说明是要flush，这时候获取sstable的实际的大小
	if builder != nil {
		sstSize = int(builder.done().size)
	}

	var (
		t   *Table
		err error
	)

	fid := utils.FID(tableName)
	//如果builder存在，那么就把buf中的内容flush到磁盘当中
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
		// 如果builder为空，说明这个时候打开的是一个存在的文件
		// 这个时候是读，不是写
	} else {
		t = &Table{lm: lm, fid: fid}
		// 这里打开sstable就是将磁盘中的数据和内存进行了交换
		// 这个时候还没进行解析，只是一个普通的字符串
		t.ss = file.OpenSSTable(&file.Options{
			FileName: tableName,
			Dir:      tableName,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
		})
	}
	// 这里进行引用，否则后续的迭代器会导致引用状态错误
	t.IncrRef()
	// 初始化sst文件，把index加载进来
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 获取sst的最大key，需要使用迭代器
	// 默认是降序
	itr := t.NewIterator(&utils.Options{})

	defer itr.Close()
	// 定位到初始位置的最大key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

func (t *Table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *Table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *Table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t tableIterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (t tableIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (t tableIterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (t tableIterator) Item() utils.Item {
	//TODO implement me
	panic("implement me")
}

func (t tableIterator) Close() error {
	//TODO implement me
	panic("implement me")
}

func (t tableIterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}
