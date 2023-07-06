// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmap'd
// read-only byte slices.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"encoding/binary"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
)

// VarintChunk holds Varint encoded sample data.
type VarintChunk struct {
	b bstream
}

// NewVarintChunk returns a new chunk with Varint encoding of the given size.
func NewVarintChunk() *VarintChunk {
	b := make([]byte, 2, 128)
	return &VarintChunk{b: bstream{stream: b, count: 0}}
}

// Encoding returns the encoding type.
func (c *VarintChunk) Encoding() Encoding {
	return EncVarint
}

// Bytes returns the underlying byte slice of the chunk.
func (c *VarintChunk) Bytes() []byte {
	return c.b.bytes()
}

// NumSamples returns the number of samples in the chunk.
func (c *VarintChunk) NumSamples() int {
	return int(binary.BigEndian.Uint16(c.Bytes()))
}

// Compact implements the Chunk interface.
func (c *VarintChunk) Compact() {
	if l := len(c.b.stream); cap(c.b.stream) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b.stream)
		c.b.stream = buf
	}
}

// Appender implements the Chunk interface.
// It is not valid to call Appender() multiple times concurrently or to use multiple
// Appenders on the same chunk.
func (c *VarintChunk) Appender() (Appender, error) {
	it := c.iterator(nil)

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() != ValNone { // nolint:revive
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &varintAppender{
		b:      &c.b,
		t:      it.t,
		v:      it.val,
		tDelta: it.tDelta,
		vDelta: it.vDelta,
	}
	return a, nil
}

func (c *VarintChunk) iterator(it Iterator) *varintIterator {
	if intIter, ok := it.(*varintIterator); ok {
		intIter.Reset(c.b.bytes())
		return intIter
	}
	return &varintIterator{
		// The first 2 bytes contain chunk headers.
		// We skip that for actual samples.
		br:       newBReader(c.b.bytes()[2:]),
		numTotal: binary.BigEndian.Uint16(c.b.bytes()),
		t:        math.MinInt64,
		val:      math.MinInt64,
	}
}

// Iterator implements the Chunk interface.
// Iterator() must not be called concurrently with any modifications to the chunk,
// but after it returns you can use an Iterator concurrently with an Appender or
// other Iterators.
func (c *VarintChunk) Iterator(it Iterator) Iterator {
	return c.iterator(it)
}

type varintAppender struct {
	b *bstream

	t      int64
	v      int64
	tDelta uint64
	vDelta uint64
}

func (a *varintAppender) AppendHistogram(int64, *histogram.Histogram) {
	panic("appended a histogram to a varint chunk")
}

func (a *varintAppender) AppendFloatHistogram(int64, *histogram.FloatHistogram) {
	panic("appended a float histogram to a varint chunk")
}

func (a *varintAppender) Append(t int64, v float64) {
	iv := int64(v)
	if float64(iv) != v {
		panic("appended a non-integer to a varint chunk")
	}

	num := binary.BigEndian.Uint16(a.b.bytes())
	if num == 0 {
		putVarbitInt(a.b, t)
		putVarbitInt(a.b, iv)
	} else {
		tDelta := uint64(t - a.t)
		tDod := int64(tDelta - a.tDelta)
		putVarbitInt(a.b, tDod)
		a.tDelta = tDelta

		vDelta := uint64(iv - a.v)
		vDod := int64(vDelta - a.vDelta)
		putVarbitInt(a.b, vDod)
		a.vDelta = vDelta
	}

	a.t = t
	a.v = iv
	binary.BigEndian.PutUint16(a.b.bytes(), num+1)
}

type varintIterator struct {
	br       bstreamReader
	numTotal uint16
	numRead  uint16

	t   int64
	val int64

	tDelta uint64
	vDelta uint64

	err error
}

func (it *varintIterator) Seek(t int64) ValueType {
	if it.err != nil {
		return ValNone
	}

	for t > it.t || it.numRead == 0 {
		if it.Next() == ValNone {
			return ValNone
		}
	}
	return ValFloat
}

func (it *varintIterator) At() (int64, float64) {
	return it.t, float64(it.val)
}

func (it *varintIterator) AtHistogram() (int64, *histogram.Histogram) {
	panic("cannot call varintIterator.AtHistogram")
}

func (it *varintIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("cannot call varintIterator.AtFloatHistogram")
}

func (it *varintIterator) AtT() int64 {
	return it.t
}

func (it *varintIterator) Err() error {
	return it.err
}

func (it *varintIterator) Reset(b []byte) {
	// The first 2 bytes contain chunk headers.
	// We skip that for actual samples.
	it.br = newBReader(b[2:])
	it.numTotal = binary.BigEndian.Uint16(b)

	it.numRead = 0
	it.t = 0
	it.val = 0
	it.tDelta = 0
	it.vDelta = 0
	it.err = nil
}

func (it *varintIterator) Next() ValueType {
	if it.err != nil || it.numRead == it.numTotal {
		return ValNone
	}

	if it.numRead == 0 {
		t, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		v, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.t = t
		it.val = v
	} else {
		tDod, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}

		it.tDelta = uint64(int64(it.tDelta) + tDod)
		it.t += int64(it.tDelta)

		vDod, err := readVarbitInt(&it.br)
		if err != nil {
			it.err = err
			return ValNone
		}
		it.vDelta = uint64(int64(it.vDelta) + vDod)
		it.val += int64(it.vDelta)
	}
	it.numRead++

	return ValFloat
}
