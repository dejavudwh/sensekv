package db

import (
	"encoding/binary"
	"reflect"
	"sensekv/utils"
	"time"
	"unsafe"
)

const (
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	ValueLogHeaderSize = 20
	vptrSize           = unsafe.Sizeof(ValuePtr{})
)

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func (p ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

func (p ValuePtr) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

// Encode encodes Pointer into byte buffer.
func (p ValuePtr) Encode() []byte {
	b := make([]byte, vptrSize)
	// Copy over the content from p to b.
	*(*ValuePtr)(unsafe.Pointer(&b[0])) = p
	return b
}

// Decode decodes the value pointer into the provided byte buffer.
func (p *ValuePtr) Decode(b []byte) {
	// Copy over data from b into p. Using *p=unsafe.pointer(...) leads to
	copy(((*[vptrSize]byte)(unsafe.Pointer(p))[:]), b[:vptrSize])
}
func IsValuePtr(e *Entry) bool {
	return e.Meta&utils.BitValuePointer > 0
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// BytesToU64 _
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

// ValuePtrCodec _
func ValuePtrCodec(vp *ValuePtr) []byte {
	return []byte{}
}

// RunCallback _
func RunCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&utils.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func DiscardEntry(e, vs *Entry) bool {
	// TODO ???????????????????????????????????? ????????????MVCC???????????????????????????????????????
	// if vs.Version != ParseTs(e.Key) {
	// 	// Version not found. Discard.
	// 	return true
	// }
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	if (vs.Meta & utils.BitValuePointer) == 0 {
		// Key also stores the value in LSM. Discard.
		return true
	}
	return false
}

type Header struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

// +------+----------+------------+--------------+-----------+
// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
// +------+----------+------------+--------------+-----------+
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode decodes the given header from the provided byte slice.
// Returns the number of bytes read.
func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += count
	h.ExpiresAt, count = binary.Uvarint(buf[index:])
	return index + count
}

// DecodeFrom reads the header from the hashReader.
// Returns the number of bytes read.
func (h *Header) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}
