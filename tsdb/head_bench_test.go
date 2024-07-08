// Copyright 2018 The Prometheus Authors
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

package tsdb

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func BenchmarkHeadStripeSeriesCreate(b *testing.B) {
	chunkDir := b.TempDir()
	// Put a series, select it. GC it and then access it.
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	opts.ChunkDirRoot = chunkDir
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(b, err)
	defer h.Close()

	lsets := make([]labels.Labels, b.N)
	hashes := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		lsets[i] = labels.FromStrings(labels.MetricName, "test_metric", "lbl", strconv.Itoa(i))
		hashes[i] = lsets[i].Hash()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, created, _ := h.getOrCreate(hashes[i], lsets[i])
		if !created {
			panic("should create new series")
		}
	}
}

func BenchmarkHeadStripeSeriesGetExisting(b *testing.B) {
	chunkDir := b.TempDir()
	// Put a series, select it. GC it and then access it.
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	opts.ChunkDirRoot = chunkDir
	opts.StripeSize = 16
	lsets := make([]labels.Labels, 1024)
	hashes := make([]uint64, 1024)
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(b, err)
	defer h.Close()

	for i := range lsets {
		lsets[i] = labels.FromStrings(labels.MetricName, "test_metric", "lbl", strconv.Itoa(i))
		hashes[i] = lsets[i].Hash()
		_, created, _ := h.getOrCreate(hashes[i], lsets[i])
		if !created {
			panic("should create new series")
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := i % 1024
		_, created, _ := h.getOrCreate(hashes[n], lsets[n])
		if created {
			panic("unexpected series creation")
		}
	}
	b.StopTimer()
}

func BenchmarkHeadStripeSeriesCreateParallel(b *testing.B) {
	chunkDir := b.TempDir()
	// Put a series, select it. GC it and then access it.
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	opts.ChunkDirRoot = chunkDir
	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(b, err)
	defer h.Close()

	var count atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lset := labels.FromStrings("a", strconv.Itoa(int(count.Inc())))
			// Use a real hash instead of just the count because the seriesHashmap relies
			// on the fact that hashes are evenly distributed across uint64 values.
			_, created, _ := h.getOrCreate(lset.Hash(), lset)
			if !created {
				panic("should create new series")
			}
		}
	})
}

func BenchmarkHeadStripeSeriesCreate_PreCreationFailure(b *testing.B) {
	chunkDir := b.TempDir()
	// Put a series, select it. GC it and then access it.
	opts := DefaultHeadOptions()
	opts.ChunkRange = 1000
	opts.ChunkDirRoot = chunkDir

	// Mock the PreCreation() callback to fail on each series.
	opts.SeriesCallback = failingSeriesLifecycleCallback{}

	h, err := NewHead(nil, nil, nil, nil, opts, nil)
	require.NoError(b, err)
	defer h.Close()

	for i := 0; i < b.N; i++ {
		h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(i)))
	}
}

type failingSeriesLifecycleCallback struct{}

func (failingSeriesLifecycleCallback) PreCreation(labels.Labels) error                     { return errors.New("failed") }
func (failingSeriesLifecycleCallback) PostCreation(labels.Labels)                          {}
func (failingSeriesLifecycleCallback) PostDeletion(map[chunks.HeadSeriesRef]labels.Labels) {}
