package utils

import "sync"

/**
前备知识： goroutine 是一种轻量级的线程，是go并发模型的核心
创建通道：ch := make(chan struct{})
发送信号：ch <- struct{}{}
接受信号：<- ch

closer.waiting.Done() // 确保在 Goroutine 结束时调用 Done，一般放在方法的第一行
closer.waiting.Wait() // 阻塞，直到所有 Goroutine 调用了 Done

*/

// Closer 用于资源回收的信号控制
type Closer struct {
	// 引入异步
	// 这里通常用于等待一组goroutine完成它们的任务
	waiting sync.WaitGroup
	// 创建通道，这里作为关闭信号的通道，当需要关闭Close对象的值的时候，向这个通道发送一个值，由于结构体是空的
	// 所以只用来传递信号，不传递实际数据
	closeSignal chan struct{}
}

// NewCloser 新建Closer
func NewCloser(i int) *Closer {
	// sync.WaitGroup{} 是 sync.WaitGroup 类型的零值，它是一个用于等待一组 Goroutine 完成的同步原语
	closer := &Closer{waiting: sync.WaitGroup{}, closeSignal: make(chan struct{})}
	// 增加等待组的计数器，计数器的初始值设置为i，这通常用于跟踪需要等待完成的Goroutine的数量
	closer.waiting.Add(i)
	return closer
}

// Close 上游通知下游协程进行资源回收，并等待协程通知完毕
func (c *Closer) Close() {
	// close用于关闭通道，关闭通道的作用是向所有正在监听该通道的协程发送一个关闭信号
	// 通知他们可以进行资源回收或者退出操作
	close(c.closeSignal)
	c.waiting.Wait()
}

// Done 表示协程已经完成了资源回收，通知上游已经关闭
func (c *Closer) Done() {
	c.waiting.Done()
}

// Wait 返回关闭信号
func (c *Closer) Wait() chan struct{} { return c.closeSignal }
