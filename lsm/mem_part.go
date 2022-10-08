package lsm

// 内存的run
// 可以是skiplist， 也可以是array
type MemRun struct {
}

// 定义key的范围，是什么作用？
// 是skiplist需要，还是栅栏需要
func NewMemRun(minKey K, maxKey K) *MemRun {
	return nil
}

func (r *MemRun) GetMin() (key K) {
	return
}

func (r *MemRun) GetMax() (key K) {
	return
}

func (r *MemRun) InsertKey(key K, value V) {
	return
}

func (r *MemRun) DeleteKey(key K) {
	return
}

type MemPart struct {
	C_0                 []*MemRun
	Filters             []*BloomFilter
	ActiveRun           int
	NumRuns             int
	EltsPerRun          int
	FracRunsMerged      float64
	BfFalsePositiveRate float64
	NumToMerge          int // ceil(_frac_runs_merged * _num_runs)
}

func NewMemPart() *MemPart {
	return nil
}
