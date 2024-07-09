// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"testing"
)

func TestLevels(t *testing.T) {
	//entrys := []*codec.Entry{
	//	{Key: []byte("key0"), Value: []byte("value0"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key1"), Value: []byte("value1"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key2"), Value: []byte("value2"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key3"), Value: []byte("value3"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key4"), Value: []byte("value4"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key5"), Value: []byte("value5"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key6"), Value: []byte("value6"), ExpiresAt: uint64(0)},
	//	{Key: []byte("key7"), Value: []byte("value7"), ExpiresAt: uint64(0)},
	//}
	//
	//// 初始化opt
	//opt := &Options{
	//	WorkDir:            "../work_test",
	//	SSTableMaxSz:       283,
	//	MemTableSize:       1024,
	//	BlockSize:          1024,
	//	BloomFalsePositive: 0.01,
	//}
	//
	//levelLive := func() {
	//	// 初始化
	//	levels := newLevelManager(opt)
	//	defer func() { _ = levels.close() }()
	//	fileName := fmt.Sprintf("%s%s", opt.WorkDir, "000001.mem")
	//	// 构件内存表
	//	imm := &memTable{
	//		wal: file.OpenWalFile(&file.Options{FileName: fileName, Dir: opt.WorkDir, Flag: os.O_CREATE | os.O_RDWR, MaxSz: int(opt.SSTableMaxSz)}),
	//		sl:  utils.NewSkipList(1024),
	//	}
	//
	//	for _, entry := range entrys {
	//		imm.set(entry)
	//	}
	//
	//	// 测试flush
	//	assert.Nil(t, levels.flush(imm))
	//	// 从levels进行get
	//	v, err := levels.get([]byte("key7"))
	//	assert.Nil(t, err)
	//	assert.Equal(t, []byte("value7"), v.Value)
	//	t.Logf("levels.Get key = %s,value = %s , expiresAt = %d", v.Key, v.Value, v.ExpiresAt)
	//	// 关闭levels
	//	assert.Nil(t, levels.close())
	//
	//}
	//
	//for i := 0; i < 10; i++ {
	//	levelLive()
	//}
}
