package bigpool

import (
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestBigpool(t *testing.T) {
	t.Run("happy case", func(t *testing.T) {
		var created int
		p := New[[]byte](func() *[]byte {
			created++
			return new([]byte)
		})

		b1 := p.Get()
		require.NotNil(t, b1)
		require.Equal(t, 1, created)
		p.Put(b1)

		b2 := p.Get()
		require.NotNil(t, b2)
		require.Equal(t, 1, created)
		require.Same(t, b1, b2)

		b3 := p.Get()
		require.Equal(t, 2, created)
		require.NotSame(t, b1, b3)
		require.NotSame(t, b2, b3)
	})

	t.Run("gc", func(t *testing.T) {
		var created int
		p := New[[]byte](func() *[]byte {
			created++
			return new([]byte)
		})

		var p1, p2 uintptr
		{
			b1 := p.Get()
			require.NotNil(t, b1)
			require.Equal(t, 1, created)
			p1 = (uintptr)(unsafe.Pointer(b1))
			p.Put(b1)
		}

		runtime.GC()

		{
			b2 := p.Get()
			require.NotNil(t, b2)
			require.Equal(t, 2, created)
			p2 = (uintptr)(unsafe.Pointer(b2))
			p.Put(b2)
		}

		require.NotEqual(t, p1, p2)
	})

	t.Run("continuous and concurrent", func(t *testing.T) {
		var created atomic.Int64
		p := New[[]byte](func() *[]byte {
			created.Inc()
			return new([]byte)
		})
		done := make(chan struct{})

		wg := sync.WaitGroup{}
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
						b := p.Get()
						slice := *b
						str := strconv.Itoa(idx)
						slice = append(slice[:0], []byte(str)...)
						assert.Equal(t, str, string(slice))
						*b = slice
						p.Put(b)
					}
				}
			}(i)
		}

		var gcs atomic.Int64
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case <-time.After(5 * time.Millisecond):
					runtime.GC()
					gcs.Inc()
				}
			}
		}()

		time.Sleep(time.Second)
		close(done)
		wg.Wait()

		t.Logf("Created=%d, GCs=%d", created.Load(), gcs.Load())
	})
}
