/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:14:09
 * @LastEditTime: 2022-07-10 11:19:21
 */
package lsm

import "sensekv/utils"

type LSM struct {
	memTable   *memTable
	immutables []*memTable
	levels     *levelManager
	option     *Options
	closer     *utils.Closer
	maxMemFID  uint32
}

type Options struct {
	WorkDir            string
	MemTableSize       int64
	SSTableMaxSz       int64
	BlockSize          int     // BlockSize is the size of each block inside SSTable in bytes.
	BloomFalsePositive float64 // BloomFalsePositive is the false positive probabiltiy of bloom filter.

	BaseLevelSize       int64
	LevelSizeMultiplier int // Determine the desired size ratio between levels
	TableSizeMultiplier int
	BaseTableSize       int64
	NumLevelZeroTables  int
	MaxLevelNum         int
}
