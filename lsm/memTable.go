package lsm

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"sensekv/db"
	"sensekv/file"
	"sensekv/utils"

	"github.com/pkg/errors"
)

const walFileExt string = ".wal"

// MemTable
type memTable struct {
	lsm        *LSM
	wal        *file.WalFile
	sl         *db.Skiplist
	buf        *bytes.Buffer
	maxVersion uint64
}

func (lsm *LSM) NewMemtable() *memTable {
	newFid := atomic.AddUint64(&(lsm.levels.maxFID), 1)
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      newFid,
		FileName: mtFilePath(lsm.option.WorkDir, newFid),
	}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: db.NewSkiplist(int64(1 << 20)), lsm: lsm}
}

// Close
func (m *memTable) close() error {
	if err := m.wal.Close(); err != nil {
		return err
	}

	return nil
}

func (m *memTable) set(entry *db.Entry) error {
	// Write to wal logs to prevent crashes
	if err := m.wal.Write(entry); err != nil {
		return err
	}
	// write to memtable
	m.sl.Add(entry)
	return nil
}

func (m *memTable) Get(key []byte) (*db.Entry, error) {
	// Index check if the current key is in the table O(1) time complexity
	// Get data from memory table
	vs := m.sl.Search(key)

	e := &db.Entry{
		Key:       key,
		Value:     vs.Value,
		ExpiresAt: vs.ExpiresAt,
		Meta:      vs.Meta,
		Version:   vs.Version,
	}

	return e, nil

}

func (m *memTable) Size() int64 {
	return m.sl.MemSize()
}

/* Recover data from wal files */
func (lsm *LSM) recovery() (*memTable, []*memTable) {
	// Get all files from the working directory
	files, err := ioutil.ReadDir(lsm.option.WorkDir)
	if err != nil {
		utils.Panic(err)
		return nil, nil
	}
	var fids []uint64
	maxFid := lsm.levels.maxFID
	// Identify files with .wal suffix
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), walFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseUint(file.Name()[:fsz-len(walFileExt)], 10, 64)
		// Consider the existence of the wal file Update maxFid
		if maxFid < fid {
			maxFid = fid
		}
		if err != nil {
			utils.Panic(err)
			return nil, nil
		}
		fids = append(fids, fid)
	}

	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	imms := []*memTable{}
	// Iterate over the fid to do the processing
	for _, fid := range fids {
		mt, err := lsm.openMemTable(fid)
		utils.CondPanic(err != nil, err)
		if mt.sl.MemSize() == 0 {
			continue
		}
		// TODO What happens if the last jump table is not full? Wouldn't that be a waste of space?
		imms = append(imms, mt)
	}
	// To update the final maxfid, the initialization must be executed serially, so no atomic operation is required
	lsm.levels.maxFID = maxFid
	return lsm.NewMemtable(), imms
}

func (lsm *LSM) openMemTable(fid uint64) (*memTable, error) {
	fileOpt := &file.Options{
		Dir:      lsm.option.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lsm.option.MemTableSize),
		FID:      fid,
		FileName: mtFilePath(lsm.option.WorkDir, fid),
	}
	s := db.NewSkiplist(int64(1 << 20))
	mt := &memTable{
		sl:  s,
		buf: &bytes.Buffer{},
		lsm: lsm,
	}
	mt.wal = file.OpenWalFile(fileOpt)
	err := mt.UpdateSkipList()
	utils.CondPanic(err != nil, errors.WithMessage(err, "while updating skiplist"))
	return mt, nil
}
func mtFilePath(dir string, fid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d%s", fid, walFileExt))
}

/* The codec operation for wal files is actually performed by wal. */
func (m *memTable) UpdateSkipList() error {
	if m.wal == nil || m.sl == nil {
		return nil
	}
	endOff, err := m.wal.Iterate(true, 0, m.replayFunction(m.lsm.option))
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("while iterating wal: %s", m.wal.Name()))
	}
	// truncate
	return m.wal.Truncate(int64(endOff))
}

func (m *memTable) replayFunction(opt *Options) func(*db.Entry, *db.ValuePtr) error {
	return func(e *db.Entry, _ *db.ValuePtr) error { // Function for replaying.
		if ts := utils.ParseTs(e.Key); ts > m.maxVersion {
			m.maxVersion = ts
		}
		m.sl.Add(e)
		return nil
	}
}
