package lsm

import (
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
)

/*
*
memTable 主要是内存上的，用来存取内存上的数据
分为两种类型，分别是可变的memTable和不可变的immuTable
执行write-ahead-log策略，新建的时候首先看有没有waf文件，有的话就先讲这部分文件加载到memTable中
*/
type memTable struct {
	// 用于实现wal操作
	wal *file.WALFile
	//跳表
	sl *utils.SkipList
}

func (m *memTable) close() error {

	if err := m.wal.Close(); err != nil {
		return err
	}
	if err := m.sl.Close(); err != nil {
		return err
	}
	return nil
}

// Set 写到memtable中去
func (m *memTable) set(entry *codec.Entry) error {
	// memtable执行waf策略，首先写到wal文件当中\
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// 写到memtable中
	if err := m.sl.Insert(entry); err != nil {
		return err
	}
	return nil
}

// Get 获取Get
func (m *memTable) get(key []byte) (*codec.Entry, error) {
	//从跳表当中获得
	return m.sl.Search(key), nil
}

func newMemTable(opt *Options) (*memTable, []*memTable) {
	/**
	这里不能用：fileOpt := &file.Options{name: "hello"}
	因为name是未导出的，相当于Java中的private，不能在别的包中进行使用
	想要使用的话，将name变为Name
	*/
	fileOpt := &file.Options{}
	//判断是否有wal文件，如果有的话就进行恢复
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}

}
