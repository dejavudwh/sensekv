/*
 * @Author: dejavudwh
 * @Date: 2022-07-11 06:53:15
 * @LastEditTime: 2022-07-11 06:55:13
 */
package utils

import (
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

func Int63n(n int64) int64 {
	mu.Lock()
	res := r.Int63n(n)
	mu.Unlock()
	return res
}

func RandN(n int) int {
	mu.Lock()
	res := r.Intn(n)
	mu.Unlock()
	return res
}

func Float64() float64 {
	mu.Lock()
	res := r.Float64()
	mu.Unlock()
	return res
}

// 生成随机字符串作为key和value
func RandStr(length int) string {
	// 包括特殊字符,进行测试
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|©®😁😭🉑️🐂㎡硬核课堂"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}
