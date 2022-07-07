/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:17:10
 * @LastEditTime: 2022-07-07 08:22:27
 */

package db

import (
	"sensekv/utils"
	"sync/atomic"
	"unsafe"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align Nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the Node.value field is 64-bit aligned. This is necessary because
	// Node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	NodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(Node{}))
)

// Arena should be lock-free.
type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind of nil pointer.
	out := &Arena{
		// Here n must be 1, wasting a byte to simplify the determination of null in getNode
		n:          1,
		shouldGrow: true,
		buf:        make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		utils.AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	// We are keeping extra bytes in the end so that the checkptr doesn't fail. We apply some
	// intelligence to reduce the size of the Node by only keeping towers upto valid height and not
	// maxHeight. This reduces the Node's size, but checkptr doesn't know about its reduced size.
	// checkptr tries to verify that the Node of size MaxNodeSize resides on a single heap
	// allocation which causes this error: checkptr:converted pointer straddles multiple allocations
	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > (1 << 30) {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		utils.AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}
	return offset - sz
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

/*
	putNode allocates a Node in the arena.
	The Node is aligned on a pointer-sized boundary. The arena offset of the Node is returned.
*/
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + NodeAlign)
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(NodeAlign)) & ^uint32(NodeAlign)
	return m
}

/*
	Put will *copy* val into arena. To make better use of this, reuse your input
	val buffer. Returns an offset into buf. User is responsible for remembering
	size of val. We could also store this size inside arena but the encoding and
	decoding will incur some overhead.
*/
func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	utils.AssertTrue(len(key) == copy(buf, key))
	return offset
}

/*
	getNode returns a pointer to the Node located at offset. If the offset is
	zero, then the nil Node pointer is returned.
*/
func (s *Arena) getNode(offset uint32) *Node {
	// tower[h] == 0
	if offset == 0 {
		return nil
	}
	return (*Node)(unsafe.Pointer(&s.buf[offset]))
}

/*
	getKey returns byte slice at offset.
*/
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

/*
	getVal returns byte slice at offset. The given size should be just the value
	size and should NOT include the meta bytes.
*/
func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

/*
	getNodeOffset returns the offset of Node in the arena. If the Node pointer is
	nil, then the zero offset is returned.
*/
func (s *Arena) getNodeOffset(nd *Node) uint32 {
	if nd == nil {
		// nil
		return 0
	}
	// The current node memory address and the first address of the allocated memory are subtracted to get offset
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
