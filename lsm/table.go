/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:16:42
 * @LastEditTime: 2022-07-10 17:10:48
 */
package lsm

import "sensekv/file"

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {}

func (t *table) StaleDataSize() uint32 {}

/* Size is its file size in bytes */
func (t *table) Size() int64 {}
