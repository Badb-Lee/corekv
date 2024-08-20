// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hardcore-os/corekv/pb"
	"github.com/hardcore-os/corekv/utils"
)

// 归并优先级
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

// 归并目标
type targets struct {
	baseLevel int
	// level的期望大小
	targetSz []int64
	// table的期望大小
	fileSz []int64
}
type compactDef struct {
	compactorId int
	t           targets
	p           compactionPriority
	thisLevel   *levelHandler
	nextLevel   *levelHandler

	top []*table
	bot []*table

	thisRange keyRange
	nextRange keyRange
	splits    []keyRange

	thisSize int64

	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// runCompacter 启动一个compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	// 随机延迟
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	// 当定时器到期时会触发这个分支
	case <-randomDelay.C:
	// 等待随机延迟，这样的话就解决了多个协程并行压缩，频繁出现并发冲突
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	//TODO 这个值有待验证
	// 这里实现了周期执行，也就是在1s内随机启动n个compacter协程来执行压缩过程，每个协程周期性的执行压缩逻辑
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		// 这个时间到了之后传入协程id
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

// runOnce
func (lm *levelManager) runOnce(id int) bool {
	// 选择合适的level来执行，返回优先级列表
	prios := lm.pickCompactLevels()
	if id == 0 {
		// 0号协程 总是倾向于压缩l0层
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// 对于l0 无论得分多少都要运行
		} else if p.adjusted < 1.0 {
			// 对于其他level 如果等分小于 则不执行
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}
func moveL0toFront(prios []compactionPriority) []compactionPriority {
	idx := -1
	// 这样遍历的原因是，因为第0层的在压缩数组中的优先级不一定在前面，需要以这种遍历的方式获取第0层的index
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	// If idx == -1, we didn't find L0.
	// If idx == 0, then we don't need to do anything. L0 is already at the front.
	// 如果大于0，这时候直接把他提到最前面
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

// run 执行一个优先级指定的合并任务
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	// 错误处理
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 选择level的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	// 如果是0，重新选取目标level
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}
	// 创建真正的压缩计划
	cd := compactDef{
		// 压缩id
		compactorId: id,
		// 压缩的优先级
		p: p,
		// 压缩的目标层
		t: p.t,
		// 这层的level
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}
	// 以上仅仅是确定了压缩哪一层和压缩到哪一层，但是每一层的table还没确定

	// 如果是第0层 对其单独填充处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		// 执行l0层level的填充
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	// 这里也是实现了状态的维护
	// 做了资源回收
	// 难道不应该在压缩完成之后删除吗？
	defer lm.compactState.delete(cd) // Remove the ranges from compaction status.

	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	// 有了各层的期望值和目标层
	t := lm.levelTargets()
	// 创建优先级数组的函数
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level: level,
			// 原始优先级
			score: score,
			// 调整后的优先级
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// 根据l0表的table数量来对压缩提权
	// 分值的计算方法：该层的table数量/该层的table的size
	// 经过这个代码，l0层的优先级变得很高
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// 非l0 层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		// compactState就是compact的一个状态表
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// 调整得分
	// 得分调整的目的是防止优先级得分过高的层导致过于频繁的合并操作
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		// 只有前一层得分>=1的才会进行调整
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				//
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				// 变得更大
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		// todo 为什么是原始优先级?为什么不是调整后的
		// 原因如下：
		// 1、原始得分反映了每层的压缩需求，即需不需要进行压缩
		// 2、调整得分是在需要压缩的层中进行排序，确定哪个层应该有限进行压缩？平衡各层的压缩频率
		if p.score >= 1.0 {
			// 因为共享了底层数组，所以这里不需要重新分配空间
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序
	// 按照调整后的优先级进行排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// levelTargets
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			// 如果小于期望值的话，直接返回期望值
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	// 初始化默认都是最大层级
	// 记录的是目标层和填充层的size情况
	t := targets{
		// 每一层level的期望
		targetSz: make([]int64, len(lm.levels)),
		// 每个sst的期望值
		fileSz: make([]int64, len(lm.levels)),
	}
	// 从最后一个level开始计算
	// 为了减少写放大次数，从lmax层开始计算
	dbSize := lm.lastLevel().getTotalSize()
	// 从lmax层数开始，一直到l1
	for i := len(lm.levels) - 1; i > 0; i-- {
		// 由于dbSize一直都是0，所以这里每一层期望都是一样的
		// 这样的话就不是一个金字塔型了，是一个正方形
		// 这样做的主要目的是为了提权
		leveTargetSize := adjust(dbSize)
		t.targetSz[i] = leveTargetSize
		// 如果当前的level没有达到合并的要求
		if t.baseLevel == 0 && leveTargetSize <= lm.opt.BaseLevelSize {
			// baseLevel的意思就是预计要合并的目标层
			// baseLevel本质就是当不满足期望的时候，找到最大的level用来合并
			t.baseLevel = i
		}
		// 每一层的量级之差
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	// 基础的tablesize
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// l0选择memtable的size作为文件的尺寸
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			// 如果小于baselevel就填充tsz
			t.fileSz[i] = tsz
		} else {
			// table的量级之差
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	// 实现了如何选择x和y才能使整体压缩效益最大化，节省空间，提高性能
	// 但是不是很理解为什么这样寻找
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	// 这里的判断断层的依据是，基层的下一层的size小于期望size
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type thisAndNextLevelRLocked struct{}

func (lm *levelManager) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	tables := make([]*table, cd.thisLevel.numTables())
	// 复制的原因： 保证并发度
	copy(tables, cd.thisLevel.tables)
	// 这一层没有tables，也就是说没有合并的必要
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	// 如果说是最后一层level，这里就尝试lmax和lmax之间的合并
	// 因为最后基本上所有的key都会被送到lmax，
	// 所以lmax和lmax之间的合并就是增加空间利用率
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	// 这里就变成了一般的压缩策略，lx到lx+1层来进行压缩
	// 这里的排序是按照索引的最大版本号来进行排序
	lm.sortByHeuristic(tables, cd)

	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// 如果被压缩过了，则什么都不需要做
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		if len(cd.bot) == 0 {
			// 重新初始化一下
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		// 如果有重叠区间
		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
		// 压缩任务执行失败
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}
	return false
}

// compact older tables first.
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ss.Indexs().MaxVersion < tables[j].ss.Indexs().MaxVersion
	})
}
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	// 这时候就对应l0到l0、lmax到lmax的压缩
	if thisLevel == nextLevel {
		// 这两种情况邀请我们很快速的完成，
		// 如果进行并行压缩的话会增加并发冲突，对性能造成影响
		// l0 to l0 和 lmax to lmax 不做特殊处理
	} else {
		// 进行切分
		lm.addSplits(&cd)
	}
	// 追加一个空的
	// todo 为什么非要追加一个空的？
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// 删除之前先更新manifest文件
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

	// 在levelhandler中，把新的加进去，老的删除
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	defer decrRefs(cd.top)
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		// 新的创建变更
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		// 源level中的删除变更
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		// 目标level中的删除变更
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	// 源table
	topTables := cd.top
	// 目标table
	botTables := cd.bot
	// 创建迭代器
	// 这里就是不同的key之间，按照版本号升序排列，相同的key按照版本号降序排列
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []utils.Iterator {
		// Create iterators across all the tables involved first.
		var iters []utils.Iterator
		switch {
		case lev == 0:
			// 如果是l0层，首先按照fid降序排列
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		// 这是对于非l0层？
		// len(topTables) > 0 不仅确保了当前是l0层，还保证了只有有表的时候才创建
		case len(topTables) > 0:
			// 对于非l0层，topTables只有一个
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		// 这里没有合并到l0层的情况
		// 非l0层的情况就是所有的table是按照key来进行排序的
		// 既然是有序的话就创建NewConcatIterator迭代器
		// 从头到位进行迭代，实现的话就是将所有的table都放到这里面，存储一个索引，类似链表
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// 开始并行执行压缩过程
	res := make(chan *table, 3)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	// 遍历每一个分段
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		// 开启一个协程去处理子压缩
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			// 合并的iterator
			it := NewMergeIterator(newIterator(), false)
			// 除了迭代器，还会有mvcc事务，manifest，
			defer it.Close()
			// 这里其实就解决了读取sst文件到内存中会oom的影响
			lm.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	err := inflightBuilders.Finish()
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 并行的运行子压缩情况
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	// 分为5部分
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		// 最后一个range分配无穷大的空间
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		// 这里实现的是每(cd.bot/5)个合并成一个
		if i%width == width-1 {
			// 设置最大值为右区间
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleData 对表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个 sst文件中陈旧数据的数量，涉及对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// max level 和 max level 的压缩
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	// 按照过期key最多的数量来进行排序，经过此排序，脏key最多的被排在前面了
	// 如果说压缩到最后一层的时候已经过期了，这个时候lmax压缩最大的意义就是清理过期的key
	lm.sortByStaleDataSize(sortedTables, cd)

	// 判断有无脏key
	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

		// 定位table是数组中的第几个
		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			// 加入到候选数组
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		// 如果刚创建小于1h
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		// 如果脏key的size小于10M
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		// 算出合并的区间范围
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		// 如果合并的sst size需要的文件尺寸大于期望值直接终止
		// 说明这个合并之后的table size太大了
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 先尝试从l0 到lbase的压缩，如果失败则对l0自己压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	// 首先尝试L0到lbase的填充
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	// 如果失败，再尝试l0到l0
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}
	// 如果优先级低于1 则不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	// 上锁，这里上的是读锁，因为仅仅需要读取一些状态
	cd.lockLevels()
	defer cd.unlockLevels()

	// top的意思就是源level的所有tables
	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	// keyRange是对范围的封装，左右key，size尺寸
	var kr keyRange
	// cd.top[0] 是最老的文件，从最老的文件开始
	for _, t := range top {
		// 获得若干sst文件的最大key和最小key
		dkr := getKeyRange(t)
		// 如果有重叠区间
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 如果有任何一个不重合的区间存在则直接终止
			// 这里实际上就是一种权衡，既要把最老的table进行合并，又要包含最大的重叠区间范围
			break
		}
	}
	// 获取range list 的全局 range 对象
	// 源level的合并后sst的左key和右key
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	// 将源level得到的区间和目标区间进行比较，选择和目标level重叠的table
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0到l0压缩
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	// 第0层的只能由compactorId为0的来进行合并
	if cd.compactorId != 0 {
		// 只要0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	//  TODO 这里是否会导致死锁？
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	// 加读锁
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	// 对状态也加锁
	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	// 因为l0层到l0层之间的压缩是相对比较频繁的
	// 取这个时间戳的原因就是防止 正在写 或者 刚刚写好 的table
	now := time.Now()
	for _, t := range top {
		// 如果说这个table的size大于期望大小的2倍
		// 说明这个table曾经被压缩过，这个时候文件数据量太大
		// 这个时候compact的时间会变长，会早层性能抖动
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		// 如果说这个table的创建时间是在10s内，说明可能正在flush
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		// 加入候选
		out = append(out, t)
	}

	// 如果小于4，不值得进行一次压缩，这也是减少l0层的压缩对整个kv抖动的影响
	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	// infRange相当于一个无限大的区间范围，这里定义一个无限大的范围，相当于和任何区间都有冲突
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组sst的区间合并后的最大与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].ss.MinKey()
	maxKey := tables[0].ss.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].ss.MinKey(), minKey) < 0 {
			minKey = tables[i].ss.MinKey()
		}
		if utils.CompareKeys(tables[i].ss.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].ss.MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	// 这里进行倒序排列的原因如下
	// 在l0层，并不是按照key来进行排序的，是按照fid来进行排序的
	// fid大的key的版本号也就大
	// l0层按照倒序排列的话，这个时候形成排序树的时候
	// 能够减少交换次数
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
		// 这里加入default的目的是为了阻塞
	}
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	// 结束的时候进行更新
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp utils.ValuePtr
			vp.Decode(e.Value)
			// 这里是vlog的fid，而非sst的fid
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			// 判断是否过期
			isExpired := IsDeletedOrExpired(it.Item().Entry())
			// 去处版本号比较两个key之间的关系
			if !utils.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				// 这里就意味着buffer填满了
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				// 如果是值指针，就更新这个文件的脏key字节数
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		// 第一次的话会走这里
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		// key不在范围内不执行
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		// TODO 这里可能要大改，对open table的参数复制一份opt
		builder := newTableBuilerWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			// TODO 这里的sst文件需要根据level大小变化
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// checkOverlap 检查是否与下一层存在重合
func (lm *levelManager) checkOverlap(tables []*table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range lm.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// 判断是否过期 是可删除
func IsDeletedOrExpired(e *utils.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// compactStatus
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (lsm *LSM) newCompactStatus() *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	// 这里加的是写锁了
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	// 源level中有压缩状态的
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	// baselevel中有压缩状态的
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	// 这里为什么用区间的比较而不是用具体的某一个table？
	// 原因就是如果对具体的table进行加锁，在比较过程中会插入新的table，
	// 就比如区间是[1,20]，其实里面就只有两个table，分别是[1,10],[15,20]
	// 如果这时候对具体的table加锁，则可以插入[11,13]，所以还是对区间进行加锁，相当于一个间隙锁
	// 但是l0不会出现插入新的table的情况，因为l0层是append操作
	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}
	// Check whether this level really needs compaction or not. Otherwise, we'll end up
	// running parallel compactions for the same level.
	// Update: We should not be checking size here. Compaction priority already did the size checks.
	// Here we should just be executing the wish of others.

	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize
	for _, t := range append(cd.top, cd.bot...) {
		// 这里仅仅是一个添加的过程，暂时还没用到
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps.
	if r.isEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.isEmpty() {
		return false
	}
	// 是否已经合并过了
	if r.inf || dst.inf {
		return true
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	// If my left is greater than dst right, we have no overlap.
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	// If my right is less than dst left, we have no overlap.
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}
