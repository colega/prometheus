package chunkenc

import (
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarintChunkHappyCase(t *testing.T) {
	type sample struct {
		t int64
		v float64
	}

	input := []sample{
		{0, 0},
		{1, 1},
		{2, 2},
		{3, 3},
		{50, 100},
		{100, 200},
		{200, 300},
		{201, 400},
		{202, 500},
		{300, 700},
		{400, 701},
		{500, 500},
		{600, 200},
	}

	chk := NewVarintChunk()
	app, err := chk.Appender()
	require.NoError(t, err)

	for _, s := range input {
		app.Append(s.t, s.v)
	}

	it := chk.Iterator(nil)
	for i, s := range input {
		require.Equal(t, ValFloat, it.Next())
		ts, val := it.At()
		require.Equal(t, s.t, ts, "Wrong timestamp at index %d", i)
		require.Equal(t, s.v, val, "Wrong value at index %d", i)
	}
	require.Equal(t, ValNone, it.Next())
}

func TestCompareChunkSizeWithMonotonicVarintSamples(t *testing.T) {
	size := func(chunk Chunk, generator func(int64, float64) (int64, float64)) int {
		app, err := chunk.Appender()
		require.NoError(t, err)

		ts, v := generator(0, 0)
		for i := 0; i < 120; i++ {
			ts, v = generator(ts, v)
			app.Append(ts, v)
		}

		return len(chunk.Bytes())
	}

	for _, tc := range []struct {
		tsDelta  int64
		tsJitter int64
		vDelta   int64
		vJitter  int64
	}{
		{tsDelta: 1, tsJitter: 0, vDelta: 0, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 0, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 10, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e2, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e3, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e4, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e5, vJitter: 0},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 10, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e2, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e3, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e4, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e5, vJitter: 1},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e2, vJitter: 10},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e3, vJitter: 10},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e4, vJitter: 10},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e5, vJitter: 10},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e4, vJitter: 100},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e6, vJitter: 100},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e4, vJitter: 100},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e7, vJitter: 100},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e7, vJitter: 1000},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e7, vJitter: 10000},
		{tsDelta: 30e3, tsJitter: 0, vDelta: 1e3, vJitter: 1e4},
	} {
		generator := func(t int64, v float64) (int64, float64) {
			return t + tc.tsDelta + rand.Int63n(tc.tsJitter+1), v + float64(tc.vDelta) + float64(rand.Int63n(tc.vJitter+1))
		}
		const iterations = 1000
		var xor, varint int
		for i := 0; i < iterations; i++ {
			xor += size(NewXORChunk(), generator)
			varint += size(NewVarintChunk(), generator)
		}
		xor /= iterations
		varint /= iterations

		pct := 100.0 - float64(xor)/float64(varint)*100
		name := fmt.Sprintf("tsDelta=%d tsJitter=%d vDelta=%d vJitter=%d", tc.tsDelta, tc.tsJitter, tc.vDelta, tc.vJitter)
		t.Logf("%60s (120 samples): xor=%-3d bytes (%0.2f bytes/sample), varint=%-3d bytes (%0.2f bytes/sample) %0.2f%% difference",
			name, xor, float64(xor)/120.0, varint, float64(varint)/120.0, pct,
		)
	}
}

func TestVarintChunkPanics(t *testing.T) {
	t.Run("with non float", func(t *testing.T) {
		chk := NewVarintChunk()
		app, err := chk.Appender()
		require.NoError(t, err)

		require.Panics(t, func() { app.Append(0, 0.5) })
	})
}

func benchmarkVarintIterator(b *testing.B, newChunk func() Chunk) {
	const samplesPerChunk = 250
	var (
		t   = int64(1234123324)
		v   = 1243535.0
		exp []pair
	)
	for i := 0; i < samplesPerChunk; i++ {
		// t += int64(rand.Intn(10000) + 1)
		t += int64(1000)
		// v = rand.Float64()
		v += float64(100)
		exp = append(exp, pair{t: t, v: v})
	}

	chunk := newChunk()
	{
		a, err := chunk.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}
			a.Append(p.t, p.v)
			j++
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var res float64
	var it Iterator
	for i := 0; i < b.N; {
		it := chunk.Iterator(it)

		for it.Next() == ValFloat {
			_, v := it.At()
			res = v
			i++
		}
		if it.Err() != io.EOF {
			require.NoError(b, it.Err())
		}
		_ = res
	}
}

func BenchmarkChunkIteratorMonotonicVarintSamples(b *testing.B) {
	b.Run("XOR", func(b *testing.B) {
		benchmarkVarintIterator(b, func() Chunk { return NewXORChunk() })
	})
	b.Run("Varint", func(b *testing.B) {
		benchmarkVarintIterator(b, func() Chunk { return NewVarintChunk() })
	})
}

func BenchmarkChunkAppenderMonotonicVarintSamples(b *testing.B) {
	b.Run("XOR", func(b *testing.B) {
		benchmarkAppenderWithIntegers(b, func() Chunk { return NewXORChunk() })
	})

	b.Run("Varint", func(b *testing.B) {
		benchmarkAppenderWithIntegers(b, func() Chunk { return NewVarintChunk() })
	})
}

func benchmarkAppenderWithIntegers(b *testing.B, newChunk func() Chunk) {
	var (
		t = int64(1234123324)
		v = 1243535.0
	)
	var exp []pair
	for i := 0; i < b.N; i++ {
		// t += int64(rand.Intn(10000) + 1)
		t += int64(1000)
		// v = rand.Float64()
		v += float64(100)
		exp = append(exp, pair{t: t, v: v})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var chunks []Chunk
	for i := 0; i < b.N; {
		c := newChunk()

		a, err := c.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}
			a.Append(p.t, p.v)
			i++
			j++
		}
		chunks = append(chunks, c)
	}

	fmt.Println("num", b.N, "created chunks", len(chunks))
}
