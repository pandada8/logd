package common

import (
	"sync"
)

type Worker struct {
	concurrent chan int
	finish     chan int

	num  int
	lock *sync.Mutex
}

func NewWorker(size int) *Worker {
	return &Worker{
		concurrent: make(chan int, size),
		finish:     make(chan int, 10),
		lock:       &sync.Mutex{},
	}
}

func (w *Worker) Run() {
	w.concurrent <- 1
	w.lock.Lock()
	w.num += 1
	w.lock.Unlock()
}

func (w *Worker) Done() {
	<-w.concurrent
	w.lock.Lock()
	w.num -= 1
	if w.num == 0 {
		w.finish <- 1
	}
	w.lock.Unlock()
}

func (w *Worker) Wait() {
	<-w.finish
}
