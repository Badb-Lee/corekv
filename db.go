package corekv

import (
	"corekv/iterator"
	"corekv/lsm"
	"corekv/stats"
	"corekv/utils"
	"corekv/utils/codec"
	"corekv/vlog"
)

type (
	// CoreAPI corekv 对外提供的功能接口
	CoreAPI interface {
		set(data *codec.Entry) error
		get(key []byte) (*codec.Entry, error)
		del(key []byte) error
		newIterator(opt *iterator.Iterator) iterator.Iterator
		info() *stats.Stats
		close() error
	}

	// DB db对外暴露的接口，全剧唯一，拥有各种资源的句柄
	DB struct {
		opt   *stats.Options
		lsm   *lsm.LSM
		vlog  *vlog.VLog
		stats *stats.Stats
	}
)

func Open(opt *stats.Options) *DB {
	db := &DB{opt: opt}
	db.lsm = lsm.NewLSM(&lsm.Options{})
	db.vlog = vlog.NewVLog(&vlog.Options{})
	db.stats = stats.NewStats(&stats.Options{})

	//启动以下三个协程
	// 启动sstable的合并压缩过程
	go db.lsm.StartMerge()
	// 启动vlog的gc过程
	go db.vlog.StartGC()
	// 启动info的统计信息
	go db.stats.StartStats()

	return db
}

func (db *DB) Close() error {
	if err := db.stats.Close(); err != nil {
		return err
	}
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) Del(key []byte) error {
	// 这里的删除不是真的删除，而是写入一个value为nil的entry
	// 不进行真正删除的原因是真正的删除会进行数据的移动，性能方面会损耗，但是这种方法也会浪费一部分内存
	return db.Set(&codec.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})

}

/*
*
以下代码可以看出kv分离的核心
如果value足够小，不用进行kv分离
因为如果kv分离的话，需要写两次读两次
*/
func (db *DB) Set(data *codec.Entry) error {
	// 一些必要性的检查
	// 如果value大于一个阈值，创建指针，将其写入vlog中
	var valuePtr *codec.ValuePtr
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		valuePtr = codec.NewValuePtr(data)
		// 首先写入vlog里面不会有事务问题，如果写入失败，vlog会在gc阶段清理掉无效key
		if err := db.vlog.Set(data); err != nil {
			return err
		}
	}

	//写入lsm，如果写指针不为空，那么替换掉value的值
	if valuePtr != nil {
		// 把值指针进行编码
		data.Value = codec.ValuePtrCodec(valuePtr)
	}

	//对lsm进行set
	return db.lsm.Set(data)
}

func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)

	// 检查输入
	// 首先从内存当中取
	if entry, err := db.lsm.Get(key); err == nil {
		return entry, err
	}

	// 检查从lsm中拿到的是不是valuePtr，如果是就从vlog中去拿
	if entry != nil && codec.IsValuePtr(entry) {
		if entry, err := db.vlog.Get(entry); err != nil {
			return entry, err
		}
	}
	return nil, err
}

func (db *DB) Info() *stats.Stats {
	return db.stats
}
