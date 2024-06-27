package utils

import "math"

type Filter []byte

func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

func (f Filter) MayContain(h uint32) bool {
	//if len(f) < 2 {
	//	return false
	//}
	//
	//k := f[len(f)-1]
	//if k > 30 {
	//	return true
	//}
	//
	//nbits := uint32(8 * (len(f) - 1))
	//delta := h>>17 | h<<15
	//for j := uint32(0); j < uint32(k); j++ {
	//	bitPos := h % uint32(nbits)
	//	if f[bitPos] == 1 {
	//		return false
	//	}
	//	h += delta
	//}
	//
	//return true

	if len(f) < 2 {
		return false
	}
	k := f[len(f)-1]
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (len(f) - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true

}

func NewFilter(keys []uint32, bitPerKey int) Filter {
	return Filter(appendFilter(keys, bitPerKey))
}

// 给定了能接受的假阳性率和确定的插入个数来计算需要的数组大小
// 在得到数组的大小m之后，计算m/n的值
func BitsPerKey(numEntries int, fp float64) int {
	// 根据公式进行计算
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// 根据公式计算出最佳的Hash函数数量
func CalcHashNum(bitsPerKey int) (k uint32) {
	k = uint32(float64(bitsPerKey) * 0.69)

	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return
}

// 将key添加到数组中
func appendFilter(key []uint32, bitsPerKey int) []byte {
	//if bitsPerKey < 0 {
	//	bitsPerKey = 0
	//}
	//
	//k := CalcHashNum(bitsPerKey)
	//
	//nbits := len(key) * bitsPerKey
	//
	//filter := make([]byte, nbits)
	//
	//for _, h := range key {
	//	delta := h>>17 | h<<15
	//	for j := uint32(0); j < k; j++ {
	//		bitPos := h % uint32(nbits)
	//		filter[bitPos] = 1
	//		h += delta
	//	}
	//}
	//
	//return filter

	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	// 0.69 is approximately ln(2).
	// 哈希函数个数
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	// 一共需要的个数
	nBits := len(key) * int(bitsPerKey)
	// For small len(keys), we can see a very high false positive rate. Fix it
	// by enforcing a minimum bloom filter length.
	// 这里防止过小，因为如果过小的话，会造成误报率过高
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	// +1是为了记录有多少个bloom filter
	filter := make([]byte, nBytes+1)

	for _, h := range key {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			// 该hash函数在bloom过滤器中的位置
			bitPos := h % uint32(nBits)
			// bitPos/8得到在哪一行
			// bitPos%8表示在那一列
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}

	//record the K value of this Bloom Filter
	// 这里用于记录有多少个k
	filter[nBytes] = uint8(k)

	return filter
}

// Hash 对字符串进行编码
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24

	}
	return h
}
