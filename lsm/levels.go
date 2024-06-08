package lsm

import (
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
)

/*
是level的管理器
levels可以理解为此一个列表
每一层都有若干个table
table就是内存中对磁盘sstable的引用的句柄
*/
type levelManager struct {
	opt *Options
	// 存储热点数据
	cache *Cache
	// 管理sstable在第几层
	mainfest *file.Mainfest
	// 对每一层进行管理
	levels []*levelHandler
	// 资源回收的信号控制
	closer *utils.Closer
}

/*
*
levelHandler是对每一层的逻辑结构进行管理
*/
type levelHandler struct {
	levelNum int
	tables   []*Table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) get(key []byte) (*codec.Entry, error) {
	//如果是第0层
	if lh.levelNum == 0 {

	} else {

	}

	return nil, nil
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.mainfest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) loadMainfest() {
	lm.mainfest = file.OpenMainfest(&file.Options{})
}

func (lm *levelManager) build() {
	// 如果mainfest的文件是空的，就进行初始化
	lm.levels = make([]*levelHandler, 8)
	// 对每一层进行初始化
	// 对第0层初始化
	lm.levels[0] = &levelHandler{levelNum: 0, tables: []*Table{openTable(lm.opt)}}
	// 对1-7层进行初始化
	for num := 1; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{levelNum: num, tables: []*Table{openTable(lm.opt)}}
	}

	//构造cache
	lm.loadCache()
}

func (lm *levelManager) loadCache() {

}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{opt: opt}
	//读取mainfest的文件构造管理器
	lm.loadMainfest()
	//用于新建levelhandler，cache
	lm.build()
	return lm
}

// 如果immuTable中的内容到达了阈值，进行刷盘
func (lm *levelManager) flush(immuTale *memTable) error {
	return nil
}

// levelmanager的获取是根据每一层的levelhandler来进行获取的
func (lm *levelManager) get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	//L0层进行查询
	if entry, err = lm.levels[0].get(key); err != nil {
		return entry, err
	}
	for i := 1; i < utils.MaxLevelNum; i++ {
		if entry, err = lm.levels[i].get(key); err != nil {
			return entry, err
		}
	}

	return entry, nil
}
