/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 13:01:46
 * @LastEditTime: 2022-07-07 14:00:25
 */
package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const cmDepth = 4

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid parameter numCounters")
	}

	// numCounters must be a power of two
	numCounters = next2Power(numCounters)
	//  mask must be 0111....1111
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 0000,0000|0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000|0000,0000
	// 0000,0000|0000,0000|0000,0000|0000,0000
	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}

	return sketch
}

func (s *cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		// The role of mask is to make it so that the maximum value is never used,
		// see next2Power for the specific reason
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}

	return int64(min)
}

/* halves all counter values. */
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

/* Clear zeroes all counters. */
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

/*
	Fast algorithm for computing the nearest second power of x
	x = 5 return 8
	x = 110 return 128
*/
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (r cmRow) get(n uint64) byte {
	// 4 bits represent a statistical count
	// If n is an odd number, it means that the high 4 bits of 1 byte are used,
	// so you need to shift right and clear the low 4 bits with 0x0f
	return (r[n/2] >> ((n & 1) * 4)) & 0x0f
}

func (r cmRow) increment(n uint64) {
	// 4 bits represent a statistical count
	// If n is an odd number, it means that the high 4 bits of 1 byte are used,
	// so you need to shift right and clear the low 4 bits with 0x0f
	i := n / 2
	s := (n & 1) * 4
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += (1 << s)
	}
}

func (r cmRow) reset() {
	// r[i] is halved
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}

func (r cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	s = s[:len(s)-1]
	return s
}
