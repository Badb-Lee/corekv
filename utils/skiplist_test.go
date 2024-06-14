package utils

//import (
//	"corekv/utils/codec"
//	"fmt"
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//	"sync"
//	"testing"
//)
//
///*
//	需要的知识：
//1、b *testing.B是基准测试，主要目的是为了衡量代码性能，t *testing.T是单元测试，主要目的是为了验证代码正确性
//2、go 关键字用于启动一个新的协程
//3、基准测试函数名以Benchmark开头，单元测试必须以Test开头
//
//*/
//
//func TestSkipList_compare(t *testing.T) {
//	list := SkipList{
//		header:   nil,
//		rand:     nil,
//		maxLevel: 0,
//		length:   0,
//	}
//
//	byte1 := []byte("1")
//	byte2 := []byte("2")
//	entry1 := codec.NewEntry(byte1, byte2)
//
//	byte1score := list.calcScore(byte1)
//	byte2score := list.calcScore(byte2)
//
//	elem := &Element{
//		levels: nil,
//		entry:  entry1,
//		score:  byte2score,
//	}
//	assert.Equal(t, -1, list.compare(byte1score, byte1, elem))
//}
//
//func TestSkipListBasicCRUD(t *testing.T) {
//	list := NewSkipList()
//
//	// 插入
//	entry1 := codec.NewEntry([]byte("key1"), []byte("value1"))
//	assert.Nil(t, list.Add(entry1))
//	// 查找
//	assert.Equal(t, entry1.Value, list.Search(entry1.Key).Value)
//
//	// 插入
//	entry2 := codec.NewEntry([]byte("key2"), []byte("value2"))
//	assert.Nil(t, list.Add(entry2))
//	// 查找
//	assert.Equal(t, entry2.Value, list.Search(entry2.Key).Value)
//
//	// 插入
//	entry3 := codec.NewEntry([]byte("key3"), []byte("value3"))
//	assert.Nil(t, list.Add(entry3))
//
//	// 插入
//	entry4 := codec.NewEntry([]byte("key4"), []byte("value4"))
//	assert.Nil(t, list.Add(entry4))
//
//	// 插入
//	entry5 := codec.NewEntry([]byte("key5"), []byte("value5"))
//	assert.Nil(t, list.Add(entry5))
//
//	// 插入
//	entry6 := codec.NewEntry([]byte("key6"), []byte("value6"))
//	assert.Nil(t, list.Add(entry6))
//
//	// 查找不存在的元素
//	assert.Nil(t, list.Search([]byte("notexists")))
//
//	// 更新
//	entry1_new := codec.NewEntry([]byte("key1"), []byte("value_new_1"))
//	assert.Nil(t, list.Add(entry1_new))
//	assert.Equal(t, entry1_new.Value, list.Search(entry1_new.Key).Value)
//
//}
//
//// 批量插入查找
//func Benchmark_SkipListBasicCRUD(b *testing.B) {
//	list := NewSkipList()
//	key, value := "", ""
//	maxTime := 1000000
//	for i := 0; i < maxTime; i++ {
//		key, value = fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
//		entry := codec.NewEntry([]byte(key), []byte(value))
//		assert.Equal(b, nil, list.Add(entry))
//		//searchVal := list.Search([]byte(key))
//		//assert.Equal(b, searchVal.Value, entry.Value)
//		//assert.Equal(b, []byte(value), searchVal.Value)
//	}
//}
//
///*
//这里批量更新时，每次更新的结果都不一样的原因如下：
//这里只能保证在更新时，不会被其他线程所占有，但是并不能保证更新的顺序
//*/
//func TestConcurrentBasic(t *testing.T) {
//	const n = 1000
//	list := NewSkipList()
//	var wg sync.WaitGroup
//	key := func(i int) []byte {
//		return []byte(fmt.Sprintf("key%d", i))
//	}
//	list.Add(codec.NewEntry(key(1), key(1)))
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			assert.Nil(t, list.Add(codec.NewEntry(key(1), key(i))))
//		}(i)
//	}
//
//	fmt.Println(list.Search(key(1)).Value)
//
//	wg.Wait()
//}
//
//func TestConcurrentBasicProved(t *testing.T) {
//	const n = 1000000
//	const numWorkers = 10 // 使用10个worker goroutine
//	list := NewSkipList()
//	var wg sync.WaitGroup
//	key := func(i int) []byte {
//		return []byte(fmt.Sprintf("key%d", i))
//	}
//
//	// 创建工作池
//	tasks := make(chan int, n)
//	for i := 0; i < numWorkers; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			for i := range tasks {
//				assert.Nil(t, list.Add(codec.NewEntry(key(i), key(i))))
//			}
//		}()
//	}
//
//	// 发送任务
//	for i := 0; i < n; i++ {
//		tasks <- i
//	}
//	close(tasks)
//
//	wg.Wait()
//}
//
//// 多线程更新的时候，每次更新的结果都不一样，原因：没有加锁
//func Benchmark_ConcurrentBasic(b *testing.B) {
//	const n = 1000
//	list := NewSkipList()
//	var wg sync.WaitGroup
//	key := func(i int) []byte {
//		return []byte(fmt.Sprintf("key%d", i))
//	}
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			// t和b有什么区别
//			assert.Nil(b, list.Add(codec.NewEntry(key(i), key(i))))
//		}(i)
//	}
//
//	wg.Wait()
//
//	for i := 0; i < n; i++ {
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			// t和b有什么区别
//			v := list.Search(key(i))
//			if v != nil {
//				require.EqualValues(b, key(i), v.Value)
//				return
//			}
//			require.NotNil(b, v)
//		}(i)
//	}
//
//	wg.Wait()
//}
