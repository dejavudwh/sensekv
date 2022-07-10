/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:09:39
 * @LastEditTime: 2022-07-10 12:49:29
 */
package lsm

import (
	senseCache "sensekv/cache"
)

const defaultCacheSize = 1024

type blockCache struct {
	indexs *senseCache.Cache // key = fid, value = table
}

func newCache(opt *Options) *blockCache {
	return &blockCache{
		indexs: senseCache.NewCache(defaultCacheSize),
	}
}

func (c *blockCache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
