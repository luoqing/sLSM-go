package lsm

// 内存的run
// 可以是skiplist， 也可以是array
type ArrayRun struct {
	data     []KVPair
	min      K
	max      K
}

// 定义key的范围，是什么作用？
// 是skiplist需要，感觉是ckiplist需要
func NewArrayRun(minKey K, maxKey K) *ArrayRun {
	r := &ArrayRun{
		min: minKey,
		max:  maxKey,
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
	return
}

func (r *ArrayRun) DeleteKey(key K) {
	return
}

func (r *ArrayRun) binarySearch(key K) (found bool, index int) {
	return
}

func (r *ArrayRun) LookUp(key K) (found bool, value V) {
	
	return
}

func (r *ArrayRun) NumElements() (num int) {
	return len(r.data)
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
		NumToMerge:     int(float64(numRuns) * fracRunsMerged),
		ActiveRun:      0,
	}
	for i := 0; i < numRuns; i++ {
		m.C_0[i] = NewArrayRun(INT32_MIN, INT32_MAX)
		m.Filters[i] = NewBloomFilter(uint64(eltsPerRun), bfFalsePositiveRate)
	}
	return m
}
