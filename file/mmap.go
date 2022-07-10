/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 05:17:24
 * @LastEditTime: 2022-07-10 07:50:13
 */
package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"sensekv/file/mmap"
)

/*
	MMapFile represents an mmapd file and includes both the buffer to the data and the file descriptor.
*/
type MMapFile struct {
	Data []byte
	Fd   *os.File
}

/*
	Open the file and get some information, the real memory mapping is done by openMMapFileUsing.
*/
func OpenMMapFile(filename string, flag int, maxSz int) (*MMapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}

	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	// If the file is opened, the original size of the file is used
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return openMMapFileUsing(fd, maxSz, writable)
}

func openMMapFileUsing(fd *os.File, sz int, writable bool) (*MMapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	buf, err := mmap.MMap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", fd.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}

	return &MMapFile{
		Data: buf,
		Fd:   fd,
	}, rerr
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (m *MMapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

/* Append a buffer to memory, remap if there is not enough space, expand the space */
func (m *MMapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := size
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

/*
	Bytes returns data starting from offset off of size sz. If there's not enough data, it would
	return nil slice and io.EOF.
*/
func (m *MMapFile) Bytes(offset, sz int) ([]byte, error) {
	if len(m.Data[offset:]) < sz {
		return nil, io.EOF
	}
	return m.Data[offset : offset+sz], nil
}

/*
	Slice returns the slice at the given offset.
	The difference with Bytes is that it will read the length from memory first
*/
func (m *MMapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

const oneGB = 1 << 30

/*
	AllocateSlice allocates a slice of the given size at the given offset.
*/
func (m *MMapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// If the file is too small, double its size or increase it by 1GB, whichever is smaller.
	if start+sz > len(m.Data) {
		growBy := len(m.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

func (m *MMapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

/* Compatible interfaces */
func ReName(name string) error {
	return nil
}

func (m *MMapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func (m *MMapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MMapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.Fd.Name(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}
