package lsm

import (
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
	"sort"
	"sync"
	"sync/atomic"
)

/*
是level的管理器
levels可以理解为此一个列表
每一层都有若干个table
table就是内存中对磁盘sstable的引用的句柄
*/
type levelManager struct {
	maxFID uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt    *Options
	// 存储热点数据
	cache *Cache
	// 管理sstable在第几层
	mainfest *file.Mainfest
	// 对每一层进行管理
	levels []*levelHandler
	// 资源回收的信号控制
	closer *utils.Closer
	lsm    *LSM
}

/*
*
levelHandler是对每一层的逻辑结构进行管理
*/
type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*Table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
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
	//// 如果mainfest的文件是空的，就进行初始化
	//lm.levels = make([]*levelHandler, 8)
	//// 对每一层进行初始化
	//// 对第0层初始化
	//lm.levels[0] = &levelHandler{levelNum: 0, tables: []*Table{openTable(lm.opt)}}
	//// 对1-7层进行初始化
	//for num := 1; num < utils.MaxLevelNum; num++ {
	//	lm.levels[num] = &levelHandler{levelNum: num, tables: []*Table{openTable(lm.opt)}}
	//}
	//
	////构造cache
	//lm.loadCache()

	// 0表示切片的初始长度
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*Table, 0),
			lm:       lm,
		})
	}

	manifest := lm.mainfest
	//// 对比manifest 文件的正确性
	//if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
	//	return err
	//}

	// 逐一加载sstable中的index block构件cache
	lm.cache = newCache(lm.opt)
	// TODO 初始化的时候index 结构放在了table中，相当于全部加载到了内存，减少了一次读磁盘，但增加了内存消耗
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		lm.levels[tableInfo.Level].addSize(t) // 记录一个level的文件总大小
	}
	// 对每一层进行排序
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// 得到最大的fid值
	atomic.AddUint64(&lm.maxFID, maxFID)

}

func (lm *levelManager) loadCache() {

}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{opt: opt}
	// 读取mainfest的文件构造管理器
	lm.loadMainfest()
	// 用于新建levelhandler，cache
	// build就是将加载的sst文件的索引加载到内存当中
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

func (lh *levelHandler) add(t *Table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) addSize(t *Table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}
