package bigpool

import (
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

type Pool[T any] struct {
	create func() *T
	typ    reflect.Type

	mtx   sync.Mutex
	items map[uintptr]struct{}
}

func New[T any](create func() *T) *Pool[T] {
	return &Pool[T]{
		create: create,
		items:  make(map[uintptr]struct{}),
	}
}

func (p *Pool[T]) Get() *T {
	p.mtx.Lock()
	for k := range p.items {
		delete(p.items, k)
		ptr := unsafe.Pointer(k)
		item := (*T)(ptr)
		runtime.SetFinalizer(item, nil)
		p.mtx.Unlock()

		return item
	}
	p.mtx.Unlock()

	return p.create()
}

func (p *Pool[T]) Put(t *T) {
	ptr := (uintptr)(unsafe.Pointer(t))

	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.items[ptr] = struct{}{}

	runtime.SetFinalizer(t, func(t *T) {
		p.mtx.Lock()
		delete(p.items, ptr)
		p.mtx.Unlock()
	})
}
