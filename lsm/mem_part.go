package lsm

import (
	"fmt"
	"log"
	"math"
)

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
		// m.C_0[i] = NewArrayRun(INT32_MIN, INT32_MAX) // 这个地方可以定义是arrayrun还是skiplist
		m.C_0[i] = NewSkipListRun(INT32_MIN, INT32_MAX)

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
		log.Printf("memrun active:%d is full\n", m.ActiveRun)
		m.ActiveRun = m.ActiveRun + 1
	}
	log.Printf("insert memdata activerun:%d numElements:%d key:%v value:%v\n", m.ActiveRun, m.C_0[m.ActiveRun].NumElements(), key, value)
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
	for i := 0; i <= m.ActiveRun; i++ {
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
