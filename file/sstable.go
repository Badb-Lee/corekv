package file

import (
	"corekv/pb"
	"corekv/utils"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

/*
*
sstable对象
*/
type SSTable struct {
	lock           *sync.RWMutex
	f              *MmapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}

func OpenSSTable(opt *Options) *SSTable {
	// 生成mmap文件
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
}

// Size 返回底层文件的尺寸
func (ss *SSTable) Size() int64 {
	fileStats, err := ss.f.Fd.Stat()
	utils.Panic(err)
	return fileStats.Size()
}

// Indexs _
func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

// MinKey 当前最小的key
func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) Init() error {
	// 用于存储sstable文件中的每个部分首地址的offset是多少，首地址key是多少
	var ko *pb.BlockOffset
	var err error
	// flush的时候从offset = 0开始写，读的时候是从offset = n开始
	// 返回sstable文件里面第一个block的offset
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	// 从文件中获取创建时间
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Atimespec.Sec, statType.Atimespec.Nsec)
	// 初始化最小的key
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	// 进行复制的原因就是后续可能值会修改
	copy(minKey, keyBytes)
	ss.minKey = minKey

	// 初始化最大的key
	blockLen := len(ss.idxTables.Offsets)
	//获得最大的key
	ko = ss.idxTables.Offsets[blockLen-1]
	// 获得最后一个block的第一个key作为maxKey
	// 这里为什么是最后一个block的第一个key作为最大key，主要还是写的时候有个细节
	keyBytes = ko.GetKey()
	maxKey := make([]byte, len(keyBytes))
	copy(maxKey, keyBytes)
	ss.maxKey = maxKey

	return nil

}

func (ss *SSTable) initTable() (*pb.BlockOffset, error) {
	// 这里只加载缓冲区，因为已经完成了映射
	readPos := len(ss.f.Data)

	// 读checksum_len
	readPos -= 4
	// 获取字符串
	buf := ss.readCheckError(readPos, 4)
	// 解码获取长度
	checksumLen := int(utils.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}

	// 读取checkSum
	// 这里也是4字节，但是考虑到兼容性，直接减去了checksumLen
	readPos -= checksumLen
	expectChk := ss.readCheckError(readPos, checksumLen)

	// 读取index_len
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	// 解码获取索引长度
	ss.idxLen = int(utils.BytesToU32(buf))

	// 读取index_data
	readPos -= ss.idxLen
	// 索引起始位置
	ss.idxStart = readPos
	// 得到索引data
	data := ss.readCheckError(readPos, ss.idxLen)
	// 利用校验和来对判断索引数据是否损坏
	// 这里进行校验的主要是索引块的大小，是性能上的权衡
	if err := utils.VerifyChecksum(data, expectChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}

	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	// 解析得到keycount的值，包括block_offset、bloom_filter、max_version、key_count
	// 注意：这个keycount的意思是表示索引数据中包含的键的数量
	ss.idxTables = indexTable
	// 判断是否使用了bloomfilter
	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	// 返回第0个偏移位，其实就是block0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")

}

func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		// 越界了
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}

	// 如果mmap映射的内存小于需要的内存，也就是说没读到，这时候就需要从磁盘中读取
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}

// SetMaxKey max 需要使用table的迭代器，来获取最后一个block的最后一个key
func (ss *SSTable) SetMaxKey(maxKey []byte) {
	ss.maxKey = maxKey
}
