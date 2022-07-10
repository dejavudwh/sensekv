/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 08:11:04
 * @LastEditTime: 2022-07-10 11:01:11
 */
package file

import (
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"sensekv/protob"
	"sensekv/utils"
)

type SSTable struct {
	lock           *sync.RWMutex
	f              *MMapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *protob.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}

func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMMapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{
		f:    omf,
		fid:  opt.FID,
		lock: &sync.RWMutex{},
	}
}

func (ss *SSTable) Init() error {
	var keyOffset *protob.BlockOffset
	var err error
	if keyOffset, err = ss.initTable(); err != nil {
		return err
	}

	// Get the creation time from the file
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)
	// init min key
	keyBytes := keyOffset.GetKey()
	// need to copy
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}

/* Initialize according to the structure of the sstable, then return the offset of the first block */
func (ss *SSTable) initTable() (bo *protob.BlockOffset, err error) {
	readPos := len(ss.f.Data)
	// Read checksum len from the last 4 bytes.
	readPos -= 4
	buf := ss.readWithErrChecking(readPos, 4)
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// Read checksum
	readPos -= checksumLen
	expectedChk := ss.readWithErrChecking(readPos, checksumLen)

	// Read index len from the top of the checksum
	readPos -= 4
	buf = ss.readWithErrChecking(readPos, 4)
	ss.idxLen = int(utils.BytesToU32(buf))

	// Read index
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readWithErrChecking(readPos, ss.idxLen)
	// Here for efficiency only idx data is verified check sum
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}
	indexTable := &protob.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTables = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

func (ss *SSTable) readWithErrChecking(offset, sz int) []byte {
	buf, err := ss.read(offset, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) read(offset, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[offset:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[offset : offset+sz], nil
	}

	// If data is empty then return an empty byte slice
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(offset))
	return res, err
}

func (ss *SSTable) SetMaxKey(maxKey []byte) {
	ss.maxKey = maxKey
}

func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) Indexs() *protob.TableIndex {
	return ss.idxTables
}

func (ss *SSTable) FID() uint64 {
	return ss.fid
}

func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

func (ss *SSTable) GetCreatedAt() *time.Time {
	return &ss.createdAt
}

func (ss *SSTable) SetCreatedAt(t *time.Time) {
	ss.createdAt = *t
}

/* The following are essentially operations on mmap files */
func (ss *SSTable) Close() error {
	return ss.f.Close()
}

func (ss *SSTable) Detele() error {
	return ss.f.Delete()
}

/*
	Bytes returns data starting from offset off of size sz. If there's not enough data, it would
	return nil slice and io.EOF.
*/
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) Size() int64 {
	fileStats, err := ss.f.Fd.Stat()
	utils.Panic(err)
	return fileStats.Size()
}

func (ss *SSTable) Truncature(size int64) error {
	return ss.f.Truncature(size)
}
