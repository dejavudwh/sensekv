/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:16:42
 * @LastEditTime: 2022-07-10 11:16:56
 */
package lsm

import "sensekv/file"

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}
