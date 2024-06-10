package lsm

import (
	"corekv/utils"
	"corekv/utils/codec"
)

/*
*
db的核心数据结构
*/
type LSM struct {
	memTable  *memTable
	immuTable []*memTable
	levels    *levelManager
	option    *Options
	closer    *utils.Closer
}

// Options
type Options struct {
}

func (lsm *LSM) Close() error {
	if err := lsm.memTable.close(); err != nil {
		return err
	}
	for i := range lsm.immuTable {
		if err := lsm.immuTable[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	//等待合并过程的结束
	lsm.closer.Close()

	return nil
}

func NewLSM(opt *Options) *LSM {
	lsm := &LSM{
		option: opt,
	}
	// 启动db恢复过程，加载wal，如果没有wal则创建一个空的mem
	// 创建一个空的immusstable
	lsm.memTable, lsm.immuTable = newMemTable(opt)
	// 初始化levelmanager
	lsm.levels = newLevelManager(opt)
	// 初始化closer，用于资源回收的信号控制
	lsm.closer = utils.NewCloser(1)
	return lsm
}

func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():

		}
		//处理并发的合并控制
	}
}

func (lsm *LSM) Set(entry *codec.Entry) error {
	// 写的时候需要检查memtable是不是写满了
	// 如果写满了需要将当前memtable的内容放入immutable中
	// 否则直接写入memtable
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}

	// 检查immutable是不是需要刷盘
	// 这里需要有一个阈值判断
	for _, immutable := range lsm.immuTable {
		// 这个刷盘操作是异步执行的，性能很好
		if err := lsm.levels.flush(immutable); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*codec.Entry, error) {
	//找的顺序 memtable -> immutable -> 磁盘
	var (
		entry *codec.Entry
		err   error
	)
	// 注意这里是entry != nil
	if entry, err := lsm.memTable.get(key); entry != nil {
		return entry, err
	}

	for _, immutable := range lsm.immuTable {
		if entry, err = immutable.get(key); err != nil {
			return entry, err
		}
	}

	return lsm.levels.get(key)
}
