package cache

import (
	"math/rand"
	"time"
)

// Count-Min Sketch
const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	// hash相关操作，四个独立的hash函数
	seed [cmDepth]uint64
	// 用于位操作，在散列或者数据压缩中，对数据进行处理
	mask uint64
}

func newCMSketch(numCounters int64) *cmSketch {
	if numCounters <= 0 {
		panic("cmSketch: invalid numCounters")
	}
	numCounters = next2Power(numCounters)
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}

	return sketch
}

// 用于增加元素的计数，每个元素通过不同的hash函数映射到不同的计数器
func (s *cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// 估计一个元素的频率，通过不同的hash函数映射到多个计数器上，取最小值
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

func (s *cmSketch) Reset() {
	for _, i := range s.rows {
		i.reset()
	}
}

func (s *cmSketch) Clear() {
	for _, i := range s.rows {
		i.clear()
	}
}

// 快速计算最小2次幂
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

// BitMap的实现
// 4个bit位来当作一个计数器
// byte = 8 bits =0000,0000 = 2 counters
type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// 取第n个计数器
func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

// 第n个计数器+1
func (r cmRow) increment(n uint64) {
	// 定位到第n个计数器上
	i := n / 2
	// 右移的距离，如果偶数不移动，奇数右移四位
	s := (n & 1) * 4
	// 加入原始数据是1101,0111
	// 如果s是偶数的话就是0111
	// 如果s是奇数的话就是1101
	// 这个操作是获取这个计数器的记录值
	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] = 1 << s
	}
}

// 数据保鲜
func (r cmRow) reset() {
	// 计数减半
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

// 全部清0
func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}
