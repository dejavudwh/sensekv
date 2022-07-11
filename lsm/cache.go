/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:09:39
 * @LastEditTime: 2022-07-11 05:18:37
 */
package lsm

import (
	senseCache "sensekv/cache"
)

const defaultCacheSize = 1024

type blockCache struct {
	indexs *senseCache.Cache // key = fid, value = table
	blocks *senseCache.Cache // key = fid:blockOffset  value = block []byte
}

func newCache(opt *Options) *blockCache {
	return &blockCache{
		indexs: senseCache.NewCache(defaultCacheSize),
	}
}

func (c *blockCache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
