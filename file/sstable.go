/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 08:11:04
 * @LastEditTime: 2022-07-10 08:14:13
 */
package file

import (
	"sync"
	"time"

	"sensekv/protobuf"
)

type SSTable struct {
	lock           *sync.RWMutex
	f              *MMapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *protobuf.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}
