package lsm

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"sensekv/db"
	"sensekv/utils"

	"github.com/stretchr/testify/assert"
)

var (
	// init opt
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

	entrys = []db.Entry{
		{Key: []byte("hello world"), Value: []byte("hello world"), ExpiresAt: uint64(0)},
		{Key: []byte("hello world"), Value: []byte("hello world"), ExpiresAt: uint64(0)},
		{Key: []byte("hello world"), Value: []byte("hello world"), ExpiresAt: uint64(0)},
	}
)

/* Correctness test */
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	test := func() {
		baseTest(t, lsm, 128)
	}
	runTest(4, test)
}

func TestFlushBase(t *testing.T) {
	lsm := buildCase()
	test := func() {
		assert.Nil(t, lsm.levels.flush(lsm.memTable))
		baseTest(t, lsm, 128)
	}

	runTest(2, test)
}

func buildCase() *LSM {
	lsm := NewLSM(opt)
	for _, entry := range entrys {
		lsm.Set(&entry)
	}
	return lsm
}

/* Test graceful closure */
func TestClose(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	test := func() {
		baseTest(t, lsm, 128)
		utils.Err(lsm.Close())
		// restart
		lsm = buildLSM()
		baseTest(t, lsm, 128)
	}

	runTest(1, test)
}

/* Testing abnormal parameters */
func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}

	runTest(1, testNil)
}

func baseTest(t *testing.T, lsm *LSM, n int) {
	e := &db.Entry{
		Key:       []byte("dejavudwh"),
		Value:     []byte("hwdvuajed"),
		ExpiresAt: 123,
	}
	// Constructing random parameters
	lsm.Set(e)
	for i := 1; i < n; i++ {
		ee := db.BuildEntry()
		lsm.Set(ee)
	}

	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
}

func buildLSM() *LSM {
	// init DB Basic Test
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	lsm := NewLSM(opt)
	return lsm
}

func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
