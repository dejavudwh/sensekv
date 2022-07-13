/*
 * @Author: dejavudwh
 * @Date: 2022-07-12 10:21:39
 * @LastEditTime: 2022-07-13 15:16:21
 */
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

	"sensekv/db"
	"sensekv/protob"
	"sensekv/utils"
)

/* Represents the priority when performing merged level compression */
type compactionPriority struct {
	level        int
	score        float64
	adjusted     float64
	dropPrefixes [][]byte
	t            targets
}

/* Target level for compression */
type targets struct {
	baseLevel int
	targetSz  []int64
	fileSz    []int64
}

/* The data structures used to actually perform the compression task */
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

func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}

	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

func (lm *levelManager) runOnce(id int) bool {
	// Calculate the priority of the level want to compress
	prios := lm.pickCompactLevels()
	if id == 0 {
		// coroutines 0 always tends to compress level l0
		// Lift power level0
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// For L0 Run regardless of score
		} else if p.adjusted < 1.0 {
			// For other levels, if the equivalence is less than 1, it is not executed
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
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}
	// If idx == -1, we didn't find L0.
	// If idx == 0, then we don't need to do anything. L0 is already at the front.
	if idx > 0 {
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}
	return prios
}

func (lm *levelManager) run(id int, p compactionPriority) bool {
	// Truly perform compression tasks
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

/* really perform the compression task, select level of some tables merge to the target level */
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	if p.t.baseLevel == 0 {
		// Select the target level once more
		p.t = lm.levelTargets()
	}
	// Create a real compression plan
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// Determine which tables to compress
	// If it's level 0, align and fill them separately
	if l == 0 {
		// nextlevel: compression target level
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// If not the last layer, then just compress to the next layer
		// N compressed to N+1 simplest compression strategy
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// Delete from merge status when merge is complete
	defer lm.compactState.delete(cd) // Remove the ranges from compaction status.

	// Execution of the merger plan
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

/*
	Select the appropriate level to perform the compression merge
	and return the priority of the judgment
*/
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	// Find out which floor I'm targeting for this compact
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// weight the compression based on the number of tables in the L0 table
	// Calculate the score NumLevelZeroTables 15
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// Non-L0 level are all prioritized based on size
	for i := 1; i < len(lm.levels); i++ {
		// sst in compressed state cannot be counted
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// The score is calculated as the ratio of the size to the target sz
		// after subtracting the table being merged
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// Adjust scoring to avoid excessive scoring
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// Select only compressed content with a score greater than 1,
	// and allow special compression from L0 to L0, and allow L0 level compression
	//  alone in order to improve query performance
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// Sort by priority
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}

func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

/* Find which layer we want to compress to */
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 {
		// If it is less than the value we expect, set it to base size
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	t := targets{
		// denotes our desired threshold: the total number of bytes of all sst files at each level
		// and sstale file size
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	// 从Calculate from the last level to prevent writing amplification
	dbSize := lm.lastLevel().getTotalSize()
	// No go 0
	for i := len(lm.levels) - 1; i > 0; i-- {
		leveTargetSize := adjust(dbSize)
		t.targetSz[i] = leveTargetSize
		// If the current level does not meet the requirements for merging
		// If level 0 is not filled for the first time, it can be merged to level 6 directly.
		// Find a level with the highest level greater than the expected value
		if t.baseLevel == 0 && leveTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		// Each level of magnitude differs by one LevelSizeMultiplier
		// If level 6 dbSize is 0 at the beginning, it will always be 0 afterwards.
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	// Start calculating filesize: the expected value inside each sstable file
	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// L0 selects the size of the memtable as the size of the file
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			// doubled ten times
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// Find the last empty level as the target level to achieve cross-level merging and reduce write amplification
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// If there is a fault, the target level++
	b := t.baseLevel
	lvl := lm.levels
	// Fill each level as much as possible
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
	// Made a copy for concurrency
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	// We pick tables, so we compact older tables first. This is similar to
	// kOldestLargestSeqFirst in RocksDB.
	// 一般策略 按照最大版本排序
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
			cd.bot = []*table{}
			// 合并当前层 重新初始化 回到前面继续执行
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}
		cd.nextRange = getKeyRange(cd.bot...)

		// 检查冲突 是否有重叠区间 继续
		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}
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
	if thisLevel == nextLevel {
		// L0 to L0 and Lmax to Lmax without special treatment
	} else {
		// Slicing, splitting into subranges, and parallel merging
		lm.addSplits(&cd)
	}
	// Add an empty one
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// L: Source layer
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

	// Update the manifest file before deleting
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

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

func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) protob.ManifestChangeSet {
	changes := []*protob.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return protob.ManifestChangeSet{Changes: changes}
}

//
func newDeleteChange(id uint64) *protob.ManifestChange {
	return &protob.ManifestChange{
		Id: id,
		Op: protob.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *protob.ManifestChange {
	return &protob.ManifestChange{
		Id:    id,
		Op:    protob.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

/* Merge the sst files of two layers */
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	// src dst
	topTables := cd.top
	botTables := cd.bot
	iterOpt := &db.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []db.Iterator {
		// Create iterators across all the tables involved first.
		var iters []db.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []db.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// Start parallel execution of the compression process
	res := make(chan *table, 3)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		if err := inflightBuilders.Do(); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
		// Open a concurrent thread to handle sub-compression
		go func(kr keyRange) {
			defer inflightBuilders.Done(nil)
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	// The mapreduce method collects table handles
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// Wait here for all compression processes to complete
	err := inflightBuilders.Finish()
	// channel Resource Recovery
	close(res)
	// Wait for all builders to be flushed to disk
	wg.Wait()

	if err == nil {
		// Synchronize the disk to ensure that the data must fall to the disk
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// If an error occurs, delete the index newly created file
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// Delineating the scope of parallel compression tasks
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
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
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Find the rightmost interval of the cut
			// Set the maximum value to the right interval
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

/* Sort the sst file for the number of stale data in the table */
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个 sst文件中陈旧数据的数量，涉及对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

/* Find the table of max level and max level compression */
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	// Get compression policy by dirty key
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	// t: This table is the dirtiest table that meets the requirements
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

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
			cd.bot = append(cd.bot, newT)
			// set range
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
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
		// If the merged sst size is larger than the required file size, terminate directly
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

/* First try to compress from L0 to Level base, if that fails then compress L0 itself */
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}

	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	// lock
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// top[0] is the oldest file, starting with the oldest file
	for _, t := range top {
		dkr := getKeyRange(t)
		// Determine if there is an overlapping interval
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// terminate directly if any of the non-overlapping intervals exist
			// Only include the oldest file's largest overlapping interval at a time
			break
		}
	}
	// Get the range object of the source level
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	// and the level base interval to determine the overlap
	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	// Found the table that matches this compression
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		// Try L0-L0 compression
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}

	// cas
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// Avoid L0 to L0 resource competition as long as concurrent process 0 can execute
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	//  TODO 这里是否会导致死锁？
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// Do not compress too large sst files during L0 to L0 compression, as this can cause performance jitter
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// If the creation time of sst is less than 10s, don't recycle it.
			continue
		}
		// If the current sst is already in a compressed state, it should be ignored.
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		return false
	}
	// infRange can represent an infinite interval, i.e., always contains
	cd.thisRange = infRange
	cd.top = out

	// Avoid any merging of L0 to other level in this process
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	// L0 to L0 compression eventually compresses into a single file,
	// which greatly reduces the number of l0 layer files and reduces read amplification
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

/* The process of interval merging returns the maximum and minimum values of a set of sst's intervals after merging */
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

func iteratorsReversed(th []*table, opt *db.Options) []db.Iterator {
	out := make([]db.Iterator, 0, len(th))
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
	}
}

/* Sub-compressed files that actually perform parallel compression */
func (lm *levelManager) subcompact(it db.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// update discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *db.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp db.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			isExpired := isDeletedOrExpired(0, it.Item().Entry().ExpiresAt)
			// key is sorted in ascending order version is sorted in descending order
			// At this point, if the same key is the largest version, it can be discarded directly
			if !utils.SameKey(key, lastKey) {
				// If the key returned by the iterator is greater than the range of the current key,
				// there is no need to execute it
				// Since this is the scope of the slice, it is possible that next still exists, but is not part of the current task
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// If the estimated sst file size is exceeded, it will simply end
					break
				}
				// Change the current key to lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				// If there is no left border, the current key is given to the left border
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// update right border
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// Determine if the content is expired, and delete if it is
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	// If the key range left still exists, then seek here means that the traversal has stopped in the middle.
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		it.Rewind()
	}
	// Both here and in addKey are for loops to handle the case of truncated files that are too large
	for it.Valid() {
		key := it.Item().Entry().Key
		// Check if it is within the range
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// Parameters of the collocation table creation
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
		// Concurrent Control
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// Take full advantage of ssd's parallel write feature flush
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done(nil)
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // There is no memtable at the time of compact, just increment maxFID here.
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

/* Check if there is overlap with the next layer */
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

/* Determine if it is expired Yes can be deleted */
func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
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
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	// Check whether the interval is covered or not,
	// if it is, return false directly, which means this task failed
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
		// Record the current table in the compressed state
		cs.tables[t.fid] = struct{}{}
	}
	return true
}

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
