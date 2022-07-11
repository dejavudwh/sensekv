/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:16:42
 * @LastEditTime: 2022-07-11 05:37:50
 */
package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sensekv/db"
	"sensekv/file"
	"sensekv/protob"
	"sort"
	"sync/atomic"
	"time"

	"sensekv/utils"

	"github.com/pkg/errors"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	// For builder presence, flush buf to disk
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// If there is no builder, open an existing sst file
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// First, you have to refer to it, otherwise using iterators later will result in a reference state error
	t.IncrRef()
	// Initialize the sst file and load the index into it
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// Get the maximum key of sst need to use iterator
	itr := t.NewIterator(&db.Options{}) // Default is descending order
	defer itr.Close()
	// Positioning to the initial position is the maximum key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

// Go to load the block corresponding to sst
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var ko protob.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

/* blockCacheKey is used to store blocks in the block cache. */
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

/* Size is its file size in bytes */
func (t *table) Size() int64 { return int64(t.ss.Size()) }

func (t *table) GetCreatedAt() *time.Time {
	return t.ss.GetCreatedAt()
}
func (t *table) Delete() error {
	return t.ss.Detele()
}

/* StaleDataSize is the amount of stale data (that can be dropped by a compaction )in this SST. */
func (t *table) StaleDataSize() uint32 { return t.ss.Indexs().StaleDataSize }

type tableIterator struct {
	it       db.Item
	opt      *db.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *db.Options) db.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
func (it *tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}

	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}
func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}
func (it *tableIterator) Item() db.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

/*
Seek
Dichotomous search offsets
If idx == 0 means the key can only be in the first block block[0].MinKey <= key
Otherwise block[0].MinKey > key
If the key is not found in the block with idx-1 then it can only be in idx If neither, then the current key is no longer in this table
*/
func (it *tableIterator) Seek(key []byte) {
	var ko protob.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) offsets(ko *protob.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}
