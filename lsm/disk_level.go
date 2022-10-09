// 先完成bloomfilter --- add, find
// 再完成diskRun --- fencepointer 和mmap
// 再完成disklevel --- merge
// 内存部分简单实用数组，或者将跳跃表代码
// 最后lsm
package lsm

type DiskPart struct {
	DiskLevels       []*DiskLevel // 磁盘有多层，需要做merge
	numDiskLevels    int
	diskRunsPerLevel int
	pageSize         int
}

type DiskLevel struct {
	runs       []*DiskRun
	level      int // 当前level
	activeRun  int
	pageSize   int     // 用于fencepointer
	bf_fp      float64 // bloomfilter
	runSize    int
	numRuns    int // 每一层的run的个数固定，但是run的size越来越大，下一层的runsize = meregeSize * runsize
	meregeSize int // 一次merge多少个run
}

// bloomfilter---bf_fp
// merge---mergeSize
// fencepointer--pageSize
// 每层level有多少个run， 每个run的大小——level, numruns, runsize
func NewDiskLevel(pageSize int, level int, runSize int, numRuns int, mergeSize int, bfFp float64) *DiskLevel {
	// 初始化runs
	return nil
}

// 将runList合并成一个有序的列表，使用堆来实现合并k个有序链表
// 对于相同key取最后一个值即可。如果遇到墓碑说明要删除。但是后面
func (l *DiskLevel) addRuns(runList []*DiskRun, runLen int, lastLevel int) {
	return
}

// 当内存不够的时候要将run写入到map
// 一个run同步一个文件？
// 多个run合并后同步到一个文件
func (l *DiskLevel) addRunByArray(run []KVPair, runLen int) {
	// 为什么还要 runLen == l.runSize
	if l.activeRun < l.numRuns && runLen == l.runSize {
		l.runs[l.activeRun].WriteData(run, 0, runLen)
		l.runs[l.activeRun].ConstructIndex()
		l.activeRun = l.activeRun + 1
	}
	return

}

func freeMergedRuns(toFree []*DiskRun) {
	//对于合并的数据进行空间释放
	return
}

// if l.LevelFull getRunsToMerge
func (l *DiskLevel) getRunsToMerge() (runs []*DiskRun) {
	return
}

func (l *DiskLevel) NumElements() (nums int) {
	return
}

func (l *DiskLevel) LookUp(key K) (found bool, value V) {
	return
}

// 层满的时候要进行merge
func (l *DiskLevel) LevelFull() bool {
	return l.activeRun == l.numRuns
}

func (l *DiskLevel) LevelEmpty() bool {
	return l.activeRun == 0
}

func addRuns(runs []*DiskRun, runLen, lastLevel int) {

}
