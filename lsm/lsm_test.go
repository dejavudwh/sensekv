package lsm

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"sensekv/db"
	"sensekv/utils"
)

var (
	opt = &Options{
		WorkDir:             "../worktest",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       3,
	}
)

// TestBase 正确性测试
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	test := func() {
		// 基准测试
		baseTest(t, lsm, 128)
	}
	// 运行N次测试多个sst的影响
	runTest(1, test)
}

// TestClose 测试优雅关闭
func TestClose(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	test := func() {
		baseTest(t, lsm, 128)
		utils.Err(lsm.Close())
		// 重启后可正常工作才算成功
		lsm = buildLSM()
		baseTest(t, lsm, 128)
	}
	// 运行N次测试多个sst的影响
	runTest(1, test)
}

// 命中不同存储介质的逻辑分支测试
func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	e := BuildEntry()
	lsm.Set(e)
	// 命中内存表
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		utils.Err(err)
		utils.CondPanic(!bytes.Equal(v.Value, e.Value), fmt.Errorf("[hitMemtable] !equal(v.Value, e.Value)"))
	}
	// 命中L0层
	hitL0 := func() {
		// baseTest的测试就包含 在命中L0的sst查询
		baseTest(t, lsm, 128)
	}
	// 命中非L0层
	hitNotL0 := func() {
		// 通过压缩将compact生成非L0数据, 会命中L6层
		lsm.levels.runOnce(0)
		baseTest(t, lsm, 128)
	}
	// 命中bf
	hitBloom := func() {
		ee := BuildEntry()
		// 查询不存在的key 如果命中则说明一定不存在
		v, err := lsm.levels.levels[0].tables[0].Search(ee.Key, &ee.Version)
		utils.CondPanic(v != nil, fmt.Errorf("[hitBloom] v != nil"))
		utils.CondPanic(err != utils.ErrKeyNotFound, fmt.Errorf("[hitBloom] err != utils.ErrKeyNotFound"))
	}

	runTest(1, hitMemtable, hitL0, hitNotL0, hitBloom)
}

// Testparameter 测试异常参数
func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}
	// TODO p2 优先级的case先忽略
	runTest(1, testNil)
}

// TestCompact 测试L0到Lmax压缩
func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	ok := false
	l0TOLMax := func() {
		// 正常触发即可
		baseTest(t, lsm, 128)
		// 直接触发压缩执行
		fid := lsm.levels.maxFID + 1
		lsm.levels.runOnce(1)
		for _, t := range lsm.levels.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0TOLMax] fid not found"))
	}
	l0ToL0 := func() {
		// 先写一些数据进来
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[0].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[1].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 6, 6, 6)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 6, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		for _, t := range lsm.levels.levels[6].tables {
			if t.fid == fid {
				ok = true
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}
	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		// 构建完全相同两个压缩计划的执行，以便于百分比构建 压缩冲突
		go lsm.levels.runCompactDef(0, 0, *cd)
		lsm.levels.runCompactDef(0, 0, *cd)
		// 检查compact status状态查看是否在执行并行压缩
		isParaller := false
		for _, state := range lsm.levels.compactState.levels {
			if len(state.ranges) != 0 {
				isParaller = true
			}
		}
		utils.CondPanic(!isParaller, fmt.Errorf("[parallerCompact] not is paralle"))
	}
	// 运行N次测试多个sst的影响
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

// 正确性测试
func baseTest(t *testing.T, lsm *LSM, n int) {
	// 用来跟踪调试的
	e := &db.Entry{
		Key:       []byte("CRTS😁硬核课堂MrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 123,
	}
	//caseList := make([]*utils.Entry, 0)
	//caseList = append(caseList, e)

	// 随机构建数据进行测试
	lsm.Set(e)
	for i := 1; i < n; i++ {
		ee := BuildEntry()
		lsm.Set(ee)
		// caseList = append(caseList, ee)
	}
	// 从levels中进行GET
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
	// TODO range功能
}

// 驱动模块
func buildLSM() *LSM {
	// init DB Basic Test
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	lsm := NewLSM(opt)
	return lsm
}

// 运行测试用例
func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}

// 构建compactDef对象
func buildCompactDef(lsm *LSM, id, thisLevel, nextLevel int) *compactDef {
	t := targets{
		targetSz:  []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		fileSz:    []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		baseLevel: nextLevel,
	}
	def := &compactDef{
		compactorId: id,
		thisLevel:   lsm.levels.levels[thisLevel],
		nextLevel:   lsm.levels.levels[nextLevel],
		t:           t,
		p:           buildCompactionPriority(lsm, thisLevel, t),
	}
	return def
}

// 构建CompactionPriority对象
func buildCompactionPriority(lsm *LSM, thisLevel int, t targets) compactionPriority {
	return compactionPriority{
		level:    thisLevel,
		score:    8.6,
		adjusted: 860,
		t:        t,
	}
}

func BuildEntry() *db.Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", utils.RandStr(16), "12345678"))
	value := []byte(utils.RandStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &db.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func tricky(tables []*table) {
	// 非常tricky的处理方法，为了能通过检查，检查所有逻辑分支
	for _, table := range tables {
		table.ss.Indexs().StaleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.ss.SetCreatedAt(&t)
	}
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
