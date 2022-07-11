package file

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"sensekv/db"
	"sensekv/utils"

	"github.com/pkg/errors"
)

type WalFile struct {
	lock    *sync.RWMutex
	f       *MMapFile
	opts    *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

func (wf *WalFile) Fid() uint64 {
	return wf.opts.FID
}

func (wf *WalFile) Close() error {
	fileName := wf.f.Fd.Name()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

func (wf *WalFile) Name() string {
	return wf.f.Fd.Name()
}

func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}

func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMMapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{f: omf, lock: &sync.RWMutex{}, opts: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	utils.Err(err)
	return wf
}

func (wf *WalFile) Write(entry *db.Entry) error {
	// A simple synchronized write of the wal file is sufficient
	wf.lock.Lock()
	plen := db.WalCodec(wf.buf, entry)
	buf := wf.buf.Bytes()
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, buf))
	wf.writeAt += uint32(plen)
	wf.lock.Unlock()
	return nil
}

// Traversing wal from disk to get data
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn db.LogEntry) (uint32, error) {
	// For now, read directly from file, because it allows
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		K:            make([]byte, 10),
		V:            make([]byte, 10),
		RecordOffset: offset,
		LF:           wf,
	}
	var validEndOffset uint32 = offset
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case e.IsZero():
			break loop
		}

		var vp db.ValuePtr
		size := uint32(int(e.LogHeaderLen()) + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fi, err := wf.f.Fd.Stat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v", wf.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncature(end)
}

type SafeRead struct {
	K []byte
	V []byte

	RecordOffset uint32
	LF           *WalFile
}

func (r *SafeRead) MakeEntry(reader io.Reader) (*db.Entry, error) {
	tee := db.NewHashReader(reader)
	var h db.WalHeader
	hlen, err := h.Decode(tee)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) { // Key length must be below uint16.
		return nil, utils.ErrTruncate
	}
	kl := int(h.KeyLen)
	if cap(r.K) < kl {
		r.K = make([]byte, 2*kl)
	}
	vl := int(h.ValueLen)
	if cap(r.V) < vl {
		r.V = make([]byte, 2*vl)
	}

	e := &db.Entry{}
	e.Offset = r.RecordOffset
	e.Hlen = hlen
	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = h.ExpiresAt
	return e, nil
}
