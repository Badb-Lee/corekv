package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
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

func (s *cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
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

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// 快速计算最接近x的二次幂算法
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
// byte = uint8 = 0000,0000 = 2 counter
type cmRow []byte

// 需要的内存的计数器/2，就是我们要创建的内存数组申请的大小
func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

// 累加技术的实现
func (r cmRow) increment(n uint64) {
	// 首先需要定位到第几个counter
	i := n / 2
	// 右移距离，偶数为0，技术为4，这里计数是从0开始
	s := (n & 1) * 4
	// 这里表示取出来的是前4位空间还是后四位空间
	v := (r[i] >> s) & 0x0f
	// 如果计数值没有超过15，如果超出不作任何操作
	if v < 15 {
		r[i] += 1 << s
	}
}

// 数据保鲜
func (r cmRow) reset() {
	// 进行计数减半
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

func (r cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	s = s[:len(s)-1]
	return s
}
