package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

const (
	// 跳表中存储的指针大小
	offsetSize = int(unsafe.Sizeof(uint32(0)))
	// 用于内存对齐
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
	// 用于确定存在Arena中的最大大小
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	// 内存的使用量
	n          uint32
	shouldGrow bool
	// 字节切片，用于存储数据
	buf []byte
}

func newArena(n int64) *Arena {
	out := &Arena{
		// 初始化内存使用量为1
		n:   1,
		buf: make([]byte, n),
	}

	return out
}

// 返回的是内存的地址
func (s *Arena) allocate(sz uint32) uint32 {
	// 原子性的将sz加到s.n上去
	offset := atomic.AddUint32(&s.n, sz)

	// 如果不增长，断言现在使用的大小 < 申请的
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		// 返回偏移量，即地址
		return offset - sz
	}

	// 申请完这个之后，下一个不够了，需要重新分配内存
	if int(offset) > len(s.buf)-MaxNodeSize {
		// arena内部自定义的初始大小
		growBy := uint32(len(s.buf))
		// 如果大于1g，就设置为1g，主要是为了防止增长过大
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		// 如果小于要申请内存的大小
		if growBy < sz {
			growBy = sz
		}
		// 很熟悉的操作，想不起来了，全部copy到新申请的地方
		newBuf := make([]byte, len(s.buf)+int(growBy))
		// 将就buf的内容copy到新的buf，并断言长度是正确的
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}

	return offset - sz

}

func (s *Arena) size() int64 {
	// 确保读操作也是安全的
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) putNode(height int) uint32 {

	// 最大高度和当前高度有多少层是未使用的，未使用的乘以每个节点的大小
	// 这个值表示当前节点层级到最大高度未使用层级所对应的内存空间
	unusedSize := (maxHeight - height) * offsetSize

	// 分配给节点的实际大小
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	// 申请之后返回的内存地址
	n := s.allocate(l)
	// 节点的实际内存地址
	m := (n + uint32((nodeAlign))) & ^uint32(nodeAlign)

	return m
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := s.allocate(l)
	// 从offset开始直至切片末尾
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

func (s *Arena) getNodeOffset(n *node) uint32 {
	if n == nil {
		return 0
	}
	// 这个指针目前的位置-arena的初始位置就是偏移量
	return uint32(uintptr(unsafe.Pointer(n)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
