package lsm

import (
	"sensekv/file"
	"sensekv/utils"
	"sort"
	"sync"
	"sync/atomic"
)

type levelManager struct {
	maxFID       uint64 // 已经分配出去的最大fid，只要创建了memtable 就算已分配
	opt          *Options
	cache        *blockCache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
	lsm          *LSM
}

func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} // 反引用
	lm.opt = opt
	// load manifest file
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}

func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		})
	}

	manifest := lm.manifestFile.GetManifest()
	// Comparing the correctness of the manifest file
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	// Load the index block of the sstable one by one to build the cache
	lm.cache = newCache(lm.opt)
	var maxFID uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFID {
			maxFID = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].add(t)
		// Record the total file size of a level
		lm.levels[tableInfo.Level].addSize(t)
	}
	// Sorting each level
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// Get the maximum fid value
	atomic.AddUint64(&lm.maxFID, maxFID)
	return nil
}

type levelHandler struct {
	sync.RWMutex
	levelNum       int
	tables         []*table
	totalSize      int64
	totalStaleSize int64
	lm             *levelManager
}

func (lh *levelHandler) add(t *table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) addBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.tables = append(lh.tables, ts...)
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

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
}

func (lh *levelHandler) subtractSize(t *table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
}

func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

func (lh *levelHandler) close() error {
	for i := range lh.tables {
		if err := lh.tables[i].ss.Close(); err != nil {
			return err
		}
	}
	return nil
}
