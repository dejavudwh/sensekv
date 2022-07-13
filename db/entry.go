/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:28:01
 * @LastEditTime: 2022-07-13 12:36:17
 */
package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sensekv/utils"
	"sync/atomic"
	"time"
	"unsafe"
)

// =============== Node

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type Node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most Nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a Node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

/*
	Returns a new Node with memory allocated on the offset returned by the putNode
	putNode, putKey, and putVal all return the offset of the memory address
*/
func newNode(arena *Arena, key []byte, v ValueStruct, height int) *Node {
	// The base level is already allocated in the Node struct.
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	Node := arena.getNode(nodeOffset)
	Node.keyOffset = keyOffset
	Node.keySize = uint16(len(key))
	Node.height = uint16(height)
	Node.value = val
	return Node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func (n *Node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *Node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *Node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *Node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *Node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

/* getVs return ValueStruct stored in Node */
func (n *Node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

// =============== ValueStruct

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64 // This field is not serialized. Only for internal usage.
}

/* The value only persistence specific values and expiration time */
func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

/*
	Encode the value and write the encoded bytes to the byte
	Here the expiration time is encoded together with the value of value
*/
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

/* calculate how many bytes are needed to store x */
func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

/* The outermost write packing structure */
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}

func BuildEntry() *Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", utils.RandStr(16), "12345678"))
	value := []byte(utils.RandStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

/* EncodedSize is the size of the ValueStruct when encoded */
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(uint64(e.Meta))
	enc += sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

func (e Entry) LogHeaderLen() int {
	return e.Hlen
}

func (e *Entry) LogOffset() uint32 {
	return e.Offset
}

const (
	vptrSize = unsafe.Sizeof(ValuePtr{})
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
