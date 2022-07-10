/*
 * @Author: dejavudwh
 * @Date: 2022-07-10 11:09:39
 * @LastEditTime: 2022-07-10 11:17:54
 */
package lsm

import (
	senseCache "sensekv/cache"
)

const defaultCacheSize = 1024

type cache struct {
	indexs *senseCache.Cache // key = fid, value = table
}

func newCache(opt *Options) *cache {
	return &cache{
		indexs: senseCache.NewCache(defaultCacheSize),
	}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
