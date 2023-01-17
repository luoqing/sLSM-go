package lsm

import (
	"fmt"
	"math"
)

// 内存的run
// 可以是skiplist， 也可以是array
type ArrayRun struct {
	data []KVPair
	min  K
	max  K
}

// 定义key的范围，是什么作用？
// 是skiplist需要，感觉是ckiplist需要
func NewArrayRun(minKey K, maxKey K) *ArrayRun {
	r := &ArrayRun{
		min: minKey,
		max: maxKey,
	}
	return r
}

func (r *ArrayRun) GetMin() (key K) {
	return r.min
}

func (r *ArrayRun) GetMax() (key K) {
	return r.max
}

func (r *ArrayRun) InsertKey(key K, value V) {
	found, index := r.binarySearch(key)
	if found {
		index = index + 1
	}
	// 元素后移动
	// 在下标index前进行插入
	for i := len(r.data) - 1; i >= index; i-- {
		r.data[i+1] = r.data[i]
	}
	r.data[index] = KVPair{
		Key:   key,
		Value: value,
	}
	if moreThan(key, r.max) {
		r.max = key
	}
	if lessThan(key, r.min) {
		r.min = key
	}
	return
}

func (r *ArrayRun) DeleteKey(key K) {
	found, index := r.binarySearch(key)
	if found {
		for i := index; i < len(r.data)-1; i++ {
			r.data[i] = r.data[i+1]
		}
	}
	return
}

func (r *ArrayRun) binarySearch(key K) (found bool, index int) {
	found = false
	left := 0
	right := len(r.data) - 1

	for left <= right {
		middle := (left + right) >> 1
		if moreThan(key, r.data[middle].Key) {
			left = middle + 1
		} else if key == r.data[middle].Key {
			found = true
			index = middle
			return
		} else {
			right = middle - 1
		}
	}
	index = left
	return
}

func (r *ArrayRun) LookUp(key K) (found bool, value V) {
	found, index := r.binarySearch(key)
	if found {
		value = r.data[index].Value
	}
	return
}

func (r *ArrayRun) NumElements() (num int) {
	return len(r.data)
}

func (r *ArrayRun) GetAll() []KVPair {
	return r.data
}

func (r *ArrayRun) Range(key1 K, key2 K) (data []KVPair) {
	if moreThan(key1, key2) {
		tmp := key2
		key2 = key1
		key1 = tmp
	}
	i1, i2 := 0, 0
	if moreThan(key1, r.max) || lessThan(key2, r.min) {
		return
	}
	if !lessThan(key1, r.min) {
		_, i1 = r.binarySearch(key1)
	} else {
		i1 = 0
	}

	if moreThan(key2, r.max) {
		key2 = r.max
		i2 = len(r.data)
	} else {
		_, i2 = r.binarySearch(key2)
	}
	return r.data[i1:i2]
}

type MemPart struct {
	C_0            []Run
	Filters        []*BloomFilter
	NumRuns        int
	EltsPerRun     int
	FracRunsMerged float64
	BfFp           float64
	NumToMerge     int // ceil(_frac_runs_merged * _num_runs)
	ActiveRun      int
}

func NewMemPart(eltsPerRun, numRuns int, fracRunsMerged, bfFalsePositiveRate float64) *MemPart {
	m := &MemPart{
		C_0:            make([]Run, numRuns),
		Filters:        make([]*BloomFilter, numRuns),
		NumRuns:        numRuns,
		EltsPerRun:     eltsPerRun,
		FracRunsMerged: fracRunsMerged,
		BfFp:           bfFalsePositiveRate,
		NumToMerge:     int(math.Ceil(float64(numRuns) * fracRunsMerged)),
		ActiveRun:      0,
	}
	for i := 0; i < numRuns; i++ {
		m.C_0[i] = NewArrayRun(INT32_MIN, INT32_MAX)
		m.Filters[i] = NewBloomFilter(uint64(eltsPerRun), bfFalsePositiveRate)
	}
	return m
}

func (m *MemPart) IsFull() bool {
	if m.C_0[m.ActiveRun].NumElements() >= m.EltsPerRun {
		m.ActiveRun = m.ActiveRun + 1
	}
	if m.ActiveRun >= m.NumRuns {
		return true
	}
	return false
}

func (m *MemPart) InsertKey(key K, value V) {
	if m.C_0[m.ActiveRun].NumElements() >= m.EltsPerRun {
		m.ActiveRun = m.ActiveRun + 1
	}
	m.C_0[m.ActiveRun].InsertKey(key, value)
	m.Filters[m.ActiveRun].Add(key)
}

func (m *MemPart) LookUp(key K) (found bool, value V) {
	for i := m.ActiveRun; i >= 0; i-- {
		found, value = m.C_0[i].LookUp(key)
		if found {
			return // 不能等于删除标记
		}
	}
	return
}

func (m *MemPart) PrintElts() {
	for i := 0; i < m.ActiveRun; i++ {
		fmt.Printf("MEMORY BUFFER RUN %d \n", i)
		all := m.C_0[i].GetAll()
		for _, kv := range all {
			fmt.Printf("%v:%v ", kv.Key, kv.Value)
		}
		fmt.Println("")
	}
}

func (m *MemPart) Size() (size int) {
	size = 0
	for i := 0; i < m.ActiveRun; i++ {
		size += m.C_0[i].NumElements()
	}
	return size
}

func (m *MemPart) GetRunsToMerge() (runsToMerge []*Run, bfToMerge []*BloomFilter) {
	for i := 0; i < m.NumToMerge; i++ {
		runsToMerge = append(runsToMerge, &m.C_0[i])
		bfToMerge = append(bfToMerge, m.Filters[i])
	}
	return
}

// [todo]
func (m *MemPart) FreeMergedRuns(runsToMerge []*Run, bfToMerge []*BloomFilter) {

	return
}

func (m *MemPart) ResetMergedRuns() {
	m.ActiveRun = m.ActiveRun - m.NumToMerge
	for i := m.ActiveRun; i < m.NumRuns; i++ {
		// 需要将run的状态进行重置
		m.C_0[i] = nil
		m.C_0[i] = NewArrayRun(INT32_MIN, INT32_MAX)
		m.Filters[i] = nil
		m.Filters[i] = NewBloomFilter(uint64(m.EltsPerRun), m.BfFp)
	}
}
