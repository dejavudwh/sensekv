/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:22:17
 * @LastEditTime: 2022-07-10 11:24:17
 */
package lsm

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}
type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}
type block struct {
	offset            int // First address offset of the current block
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}
