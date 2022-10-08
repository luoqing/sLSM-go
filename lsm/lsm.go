package lsm

type LSM struct {
	MemData MemPart  // 内存只有一层
	DiskData DiskPart
	// 这个地方还要加锁, 用于merge, 保证线程安全
}

// diskRun的大小取决于合并的大小
func NewLSM(eltsPerRun int, numRuns int, mergedFrac float64, bfFp float64, pageSize int, diskRunsPerLevel) *LSM{
	// 初始化内存部分
	// 初始化磁盘部分
}

func (l *LSM)InsertKey(key int, value int) {

}

func (l *LSM)Lookup(key int) (found bool, value int){
	return
}

const (
	V_TOMBSTONE = -1
)

func (l *LSM)DeleteKey(key int) {
	l.InsertKey(key, V_TOMBSTONE)
}

// Memory Buffer
// Disk Buffer
// 输出每层的数据
func (l *LSM)PrintElts() {

}

// 输出每层的个数
// 然后PrintElts
func (l *LSM)PrintStats() {

}

// 磁盘往下合并
func (l *LSM) MergeDiskRunsToLevel(level int) {

}

// 将要merge的数据写入到disk
func (l *LSM) MergeMemRuns(runsToMerge []*Run, bfToMerge []*BloomFilter) {

}

func (l *LSM) DoMerge() {

}
