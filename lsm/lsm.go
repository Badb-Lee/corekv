package lsm

import (
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

type LSM struct {
	// 两个都是memTable，只不过一个是元素，一个是列表
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
}

// Options
type Options struct {
}

// 关闭lsm
func (lsm *LSM) Close() error {
	if err := lsm.memTable.close(); err != nil {
		return err
	}
	for i := range lsm.immutables {
		if err := lsm.immutables[i].close(); err != nil {
			return err
		}
	}
	if err := lsm.levels.close(); err != nil {
		return err
	}
	// 等待合并过程的结束
	lsm.closer.Close()
	return nil
}

// NewLSM
func NewLSM(opt *Options) *LSM {
	//首先创建lsm树
	lsm := &LSM{option: opt}
	// 启动DB恢复过程加载wal（write-ahead-log，在数据被修改之前，将修改操作记录到日志文件当中，放置数据在修改过程中发生故障），如果没有恢复内容则创建新的内存表
	// 并且创建一个空的immutable列表
	lsm.memTable, lsm.immutables = recovery(opt)
	// 初始化levelManager
	// 用于并发控制
	lsm.levels = newLevelManager(opt)
	// 初始化closer 用于资源回收的信号控制
	lsm.closer = utils.NewCloser(1)
	return lsm
}

// StartMerge
func (lsm *LSM) StartMerge() {
	defer lsm.closer.Done()
	for {
		select {
		case <-lsm.closer.Wait():
		}
		// 处理并发的合并过程
	}
}

func (lsm *LSM) Set(entry *codec.Entry) error {
	// 检查当前memtable是否写满，是的话创建新的memtable,并将当前内存表写到immutables中
	// 否则写入当前memtable中
	if err := lsm.memTable.set(entry); err != nil {
		return err
	}
	// 检查是否存在immutable需要刷盘
	// todo 需要先判断immutable达到了一个阈值
	for _, immutable := range lsm.immutables {
		// 这个flush是异步执行的，所以插入性能很好
		if err := lsm.levels.flush(immutable); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// 从内存表中查询,先查活跃表，在查不变表
	if entry, err = lsm.memTable.Get(key); entry != nil {
		return entry, err
	}
	// 不变表也是在内存中的
	for _, imm := range lsm.immutables {
		if entry, err = imm.Get(key); entry != nil {
			return entry, err
		}
	}
	// 如果说memmtable和immutable都没有，那就去磁盘中拿
	// 从level manger查询
	return lsm.levels.Get(key)
}
