package utils

import "sync"

/* Signal control for resource recovery */
type Closer struct {
	waiting     sync.WaitGroup
	CloseSignal chan struct{}
}

func NewCloser() *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.CloseSignal = make(chan struct{})
	return closer
}

/*
	Upstream notifies the downstream concurrent process of resource recovery
	and waits for the concurrent process to notify that recovery is complete
*/
func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

func (c *Closer) Done() {
	c.waiting.Done()
}

func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}
