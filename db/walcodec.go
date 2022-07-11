package db

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"sensekv/utils"
)

type LogEntry func(e *Entry, vp *ValuePtr) error

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

const maxHeaderSize int = 21

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	var err error

	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)

	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)

	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

/*
	WalCodec Write to wal file code
	| header | key | value | crc32 |
*/
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}

	hash := crc32.New(utils.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// encode header.
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	utils.Panic2(writer.Write(headerEnc[:sz]))
	utils.Panic2(writer.Write(e.Key))
	utils.Panic2(writer.Write(e.Value))
	// write crc32 hash.
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	utils.Panic2(buf.Write(crcBuf[:]))
	// return encoded length.
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

// EstimateWalCodecSize 预估当前kv 写入wal文件占用的空间大小
func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 /* ExpiresAt uint64 */ +
		crc32.Size + maxHeaderSize
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int // Number of bytes read.
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(utils.CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

/* Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure. */
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

/* ReadByte reads exactly one byte from the reader. Returns error on failure. */
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}

/* Sum32 returns the sum32 of the underlying hash. */
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}
