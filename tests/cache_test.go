/*
 * @Author: dejavudwh
 * @Date: 2022-07-08 02:20:20
 * @LastEditTime: 2022-07-08 03:46:10
 */
package tests

import (
	"fmt"
	. "sensekv/cache"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(5)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set %s: %s\n", key, cache)
	}

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			fmt.Printf("get %s: %s\n", key, cache)
			assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)
	}
	fmt.Printf("at last: %s\n", cache)
}
