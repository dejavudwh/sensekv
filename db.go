/*
 * @Author: dejavudwh
 * @Date: 2022-07-15 07:03:33
 * @LastEditTime: 2022-07-15 11:49:22
 */
package sensekv

import (
	"expvar"
	"fmt"
	"math"
	"sensekv/db"
	"sensekv/lsm"
	"sensekv/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type (
	SenseAPI interface {
		Set(data *db.Entry) error
		Get(key []byte) (*db.Entry, error)
		Del(key []byte) error
		NewIterator(opt *db.Options) db.Iterator
		Info() *Stats
		Close() error
	}

	DB struct {
		sync.RWMutex
		opt         *Options
		lsm         *lsm.LSM
		vlog        *valueLog
		stats       *Stats
		flushChan   chan flushTask // For flushing memtables.
		writeCh     chan *request
		blockWrites int32
		vhead       *db.ValuePtr
		logRotates  int32
	}
)

var (
	head = []byte("!sensekv!head") // For storing value offset for replay.
)

func Open(opt *Options) *DB {
	c := utils.NewCloser()
	db := &DB{opt: opt}
	// 初始化vlog结构
	db.initVLog()
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             opt.WorkDir,
		MemTableSize:        opt.MemTableSize,
		SSTableMaxSz:        opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0, //0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
		DiscardStatsCh:      &(db.vlog.lfDiscardStats.flushChan),
	})
	// 初始化统计信息
	db.stats = newStats(opt)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartCompacter()
	// 准备vlog gc
	c.Add(1)
	db.writeCh = make(chan *request)
	db.flushChan = make(chan flushTask, 16)
	go db.doWrites(c)
	// 启动 info 统计过程
	go db.stats.StartStats()
	return db
}

func (database *DB) Close() error {
	database.vlog.lfDiscardStats.closer.Close()
	if err := database.lsm.Close(); err != nil {
		return err
	}
	if err := database.vlog.close(); err != nil {
		return err
	}
	if err := database.stats.close(); err != nil {
		return err
	}
	return nil
}

func (database *DB) Get(key []byte) (*db.Entry, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	}

	originKey := key
	var (
		entry *db.Entry
		err   error
	)
	key = utils.KeyWithTs(key, math.MaxUint32)
	// 从LSM中查询entry，这时不确定entry是不是值指针
	if entry, err = database.lsm.Get(key); err != nil {
		return entry, err
	}
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	if entry != nil && db.IsValuePtr(entry) {
		var vp db.ValuePtr
		vp.Decode(entry.Value)
		result, cb, err := database.vlog.read(&vp)
		defer db.RunCallback(cb)
		if err != nil {
			return nil, err
		}
		entry.Value = utils.SafeCopy(nil, result)
	}

	if isDeletedOrExpired(entry) {
		return nil, utils.ErrKeyNotFound
	}
	entry.Key = originKey
	return entry, nil
}

func (database *DB) Set(data *db.Entry) error {
	if data == nil || len(data.Key) == 0 {
		return utils.ErrEmptyKey
	}
	// 做一些必要性的检查
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	var (
		vp  *db.ValuePtr
		err error
	)
	data.Key = utils.KeyWithTs(data.Key, math.MaxUint32)
	// 如果value不应该直接写入LSM 则先写入 vlog文件，这时必须保证vlog具有重放功能
	// 以便于崩溃后恢复数据
	if !database.shouldWriteValueToLSM(data) {
		if vp, err = database.vlog.newValuePtr(data); err != nil {
			return err
		}
		data.Meta |= utils.BitValuePointer
		data.Value = vp.Encode()
	}
	return database.lsm.Set(data)
}

func (database *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	return database.Set(&db.Entry{
		Key:       key,
		Value:     nil,
		ExpiresAt: 0,
	})
}

func (database *DB) Info() *Stats {
	return database.stats
}

func isDeletedOrExpired(e *db.Entry) bool {
	if e.Value == nil {
		return true
	}
	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

// RunValueLogGC triggers a value log garbage collection.
func (database *DB) RunValueLogGC(discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return utils.ErrInvalidRequest
	}
	// Find head on disk
	headKey := utils.KeyWithTs(head, math.MaxUint64)
	val, err := database.lsm.Get(headKey)
	if err != nil {
		if err == utils.ErrKeyNotFound {
			val = &db.Entry{
				Key:   headKey,
				Value: []byte{},
			}
		} else {
			return errors.Wrap(err, "Retrieving head from on-disk LSM")
		}
	}

	// 内部key head 一定是value ptr 不需要检查内容
	var head db.ValuePtr
	if len(val.Value) > 0 {
		head.Decode(val.Value)
	}

	// Pick a log file and run GC
	return database.vlog.runGC(discardRatio, &head)
}

type flushTask struct {
	mt           *db.Skiplist
	vptr         *db.ValuePtr
	dropPrefixes [][]byte
}

func (database *DB) shouldWriteValueToLSM(e *db.Entry) bool {
	return int64(len(e.Value)) < database.opt.ValueThreshold
}

func (database *DB) batchSet(entries []*db.Entry) error {
	req, err := database.sendToWriteCh(entries)
	if err != nil {
		return err
	}

	return req.Wait()
}

func (database *DB) sendToWriteCh(entries []*db.Entry) (*request, error) {
	if atomic.LoadInt32(&database.blockWrites) == 1 {
		return nil, utils.ErrBlockedWrites
	}
	var count, size int64
	for _, e := range entries {
		size += int64(e.EstimateSize(int(database.opt.ValueThreshold)))
		count++
	}
	if count >= database.opt.MaxBatchCount || size >= database.opt.MaxBatchSize {
		return nil, utils.ErrTxnTooBig
	}

	// TODO 尝试使用对象复用，后面entry对象也应该使用
	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	req.IncrRef()           // for db write
	database.writeCh <- req // Handled in doWrites.
	return req, nil
}

func (database *DB) doWrites(lc *utils.Closer) {
	// 做异步并发控制
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := database.writeRequests(reqs); err != nil {
			utils.Err(fmt.Errorf("writeRequests: %v", err))
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-database.writeCh:
		case <-lc.CloseSignal:
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*utils.KVWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-database.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.CloseSignal:
				goto closedCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-database.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

func (database *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}
	err := database.vlog.write(reqs)
	if err != nil {
		done(err)
		return err
	}
	var count int
	for _, b := range reqs {
		if len(b.Entries) == 0 {
			continue
		}
		count += len(b.Entries)
		if err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		if err := database.writeToLSM(b); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
		database.Lock()
		database.updateHead(b.Ptrs)
		database.Unlock()
	}
	done(nil)
	return nil
}
func (database *DB) writeToLSM(b *request) error {
	if len(b.Ptrs) != len(b.Entries) {
		return errors.Errorf("Ptrs and Entries don't match: %+v", b)
	}

	for i, entry := range b.Entries {
		if database.shouldWriteValueToLSM(entry) { // Will include deletion / tombstone case.
			// 需要去掉kv分离的标记 因为用户重新写入的时候的value的大小已经小于...
			entry.Meta = entry.Meta &^ utils.BitValuePointer
		} else {
			entry.Meta = entry.Meta | utils.BitValuePointer
			entry.Value = b.Ptrs[i].Encode()
		}
		database.lsm.Set(entry)
	}
	return nil
}
