/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 09:05:55
 * @LastEditTime: 2022-07-10 13:22:40
 */
package cache

import (
	"math"
	"sensekv/utils"
)

const ShortBloomLen = 30

type Filter []byte

type BloomFilter struct {
	bitmap  Filter
	hashNum uint8 // the best hash num
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	return bf.mayContain(utils.Hash(key))
}

/*
	MayContain returns whether the filter may contain given key.
	False positives are possible, where it returns true for keys not in the original set.
*/
func (bf *BloomFilter) mayContain(hashCode uint32) bool {
	if bf.Len() < 2 {
		return false
	}

	k := bf.hashNum
	if k > ShortBloomLen {
		// This is reserved for potentially new encodings for short Bloom filters. Consider it a match.
		return true
	}

	nBits := uint32(8 * (bf.Len() - 1))
	// hash func
	delta := hashCode>>17 | hashCode<<15
	// k times
	for j := uint8(0); j < k; j++ {
		bitPos := hashCode % nBits
		// bitPos/8 is to find the corresponding byte
		// bitPos%8 is to find the corresponding bit
		if (bf.bitmap[bitPos/8] & (1 << (bitPos % 8))) == 0 {
			return false
		}
		hashCode += delta
	}

	return true
}

func (bf *BloomFilter) InsertKey(key []byte) bool {
	return bf.insert(utils.Hash(key))
}

/* insert to bitmap of bloom filter */
func (bf *BloomFilter) insert(hashCode uint32) bool {
	k := bf.hashNum
	if k > ShortBloomLen {
		// This is reserved for potentially new encodings for short Bloom filters. Consider it a match.
		return true
	}
	nBits := uint32(8 * (bf.Len() - 1))
	delta := hashCode>>17 | hashCode<<15
	for j := uint8(0); j < k; j++ {
		bitPos := hashCode % uint32(nBits)
		bf.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		hashCode += delta
	}
	return true
}

/* AllowAndRecord can return true if it is already the second visit */
func (bf *BloomFilter) AllowAndRecord(h uint32) bool {
	if bf == nil {
		return true
	}
	already := bf.mayContain(h)
	if !already {
		bf.insert(h)
	}
	return already
}

func (f *BloomFilter) reset() {
	if f == nil {
		return
	}
	for i := range f.bitmap {
		f.bitmap[i] = 0
	}
}

func (bf *BloomFilter) Len() int32 {
	return int32(len(bf.bitmap))
}

const (
	ln2         = float64(0.69314718056)
	minimumBits = 64
)

/*
	newFilter returns a new Bloom filter that encodes a set of []byte keys with
	the given number of bits per key, approximately.
	A good bitsPerKey value is 10, which yields a filter with ~ 1% false positive rate.
*/
func newFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := bloomBitsPerKey(numEntries, falsePositive)
	return initFilter(numEntries, bitsPerKey)
}

func initFilter(numEntries int, bitPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitPerKey < 0 {
		bitPerKey = 0
	}
	bf.hashNum = uint8(calcHashNum(bitPerKey))
	nBits := numEntries * int(bitPerKey)
	// For small len(keys), we can see a very high false positive rate. by enforcing a minimum bloom filter length.
	if nBits < minimumBits {
		nBits = minimumBits
	}
	// Meet the length
	nBytes := (nBits + 7) / 8
	// Request an extra bit to record the K(hashNum) value of the Bloom filter
	filter := make([]byte, nBytes+1)
	filter[nBytes] = uint8(bf.hashNum)

	bf.bitmap = filter
	return bf
}

/*
	BloomBitsPerKey returns the bits per key required by bloomfilter based on
	the false positive rate.
	Calculate m/n according to the formula, m is determined under the given probability of False Positive
	and the determined amount of data numEntries
*/
func bloomBitsPerKey(numEntries int, fp float64) int {
	// m = (-1 * n * lnp) / (ln2 * ln2)
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(ln2, 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

/* Calculate nums of hash func */
func calcHashNum(bitsPerKey int) (k uint32) {
	k = uint32(float64(bitsPerKey) * ln2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	return
}
