// 先完成bloomfilter --- add, find
// 再完成diskRun --- fencepointer 和mmap
// 再完成disklevel --- merge
// 内存部分简单实用数组，或者将跳跃表代码
// 最后lsm
package lsm

import (
	"container/heap"
)

type DiskPart struct {
	DiskLevels       []*DiskLevel // 磁盘有多层，需要做merge
	numDiskLevels    int
	diskRunsPerLevel int
	pageSize         int
}

func NewDiskPart() (d *DiskPart){
	return d
}

type DiskLevel struct {
	runs      []*DiskRun
	level     int // 当前level
	activeRun int
	pageSize  int     // 用于fencepointer
	bfFp      float64 // bloomfilter
	runSize   int
	numRuns   int // 每一层的run的个数固定，但是run的size越来越大，下一层的runsize = meregeSize * runsize
	mergeSize int // 一次merge多少个run
}

// bloomfilter---bf_fp
// merge---mergeSize
// fencepointer--pageSize
// 每层level有多少个run， 每个run的大小——level, numruns, runsize
// 【easy】
func NewDiskLevel(pageSize int, level int, runSize int, numRuns int, mergeSize int, bfFp float64) *DiskLevel {
	l := &DiskLevel{
		level:     level,
		runs:      make([]*DiskRun, numRuns),
		numRuns:   numRuns,
		runSize:   runSize,
		activeRun: 0,
		pageSize:  pageSize,
		bfFp:      bfFp,
		mergeSize: mergeSize,
	}
	for i := 0; i < numRuns; i++ {
		run := NewDiskRun(runSize, pageSize, level, i, bfFp)
		l.runs[i] = run
	}
	return l
}

// 将runList合并成一个有序的列表，使用堆来实现合并k个有序链表
// 对于相同key取最后一个值即可。如果遇到墓碑说明要删除。但是后面
// 取出每个run的第一个数，写入heap，然后pop出来这个数
// 对于key相同的还是要取最后的那个
// l.runs[l.activeRun].Map
// newRunsize = 所有元素的总和
func (l *DiskLevel) addRuns(runList []*DiskRun, lastLevel bool) {
	p := make([]int, len(runList))
	pq := &PriorityQueue{}
	for i := 0; i < len(runList); i++ {
		p[i] = 0
	}
	heap.Init(pq)
	var lastKey K = runList[0].Map[0].Key
	j := -1
	for {
		for i := 0; i < len(runList); i++ {
			if p[i] < runList[i].GetCapacity() {
				item := &Item{
					KV:    runList[i].Map[p[i]],
					index: i,
				}
				heap.Push(pq, item)
				p[i] = p[i] + 1
			}
		}
		if pq.Len() > 0 {
			x := heap.Pop(pq)
			item := x.(*Item)
			key := item.KV.Key
			if j != -1 && key == lastKey {
				l.runs[l.activeRun].Map[j] = item.KV // 这个根据index进行排序了，key相同最后一个index最大，也可以取index最大的进行赋值
			} else {
				lastValue := l.runs[l.activeRun].Map[j].Value
				// 最后一层的删除标记的数据要进行删除， 上一个删除标记的数据
				if j != -1 && lastLevel && lastValue == V_TOMBSTONE {
					j --
				}
				j ++
				l.runs[l.activeRun].Map[j] = item.KV
			}
			lastKey = key
		} else {
			break
		}
	}
	// 最后一个数据如果是删除标记要进行清理
	if (lastLevel &&  l.runs[l.activeRun].Map[j].Value == V_TOMBSTONE){
		j = j -1
	}
	l.runs[l.activeRun].SetCapacity(j+1)
	l.runs[l.activeRun].ConstructIndex()
	if j + 1 > 0 {
		l.activeRun = l.activeRun + 1
	}

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

// todo:
func freeMergedRuns(toFree []*DiskRun) {
	//对于合并的数据进行空间释放, 释放
	return
}

// 获取mergesize，然后又数据的
func (l *DiskLevel) getRunsToMerge() (runs []*DiskRun) {
	for i := 0; i < l.mergeSize; i ++ {
		runs = append(runs, l.runs[i])
	}
	return
}

func (l *DiskLevel) NumElements() (nums int) {
	nums = 0
	for i := 0; i < l.numRuns; i++ {
		nums += l.runs[i].GetCapacity()
	}
	return
}

// 每层run去lookup
// 注意从最近的进行查询
func (l *DiskLevel) LookUp(key K) (found bool, value V) {
	searchRuns := l.numRuns - 1;
	if !l.LevelFull() {
		searchRuns = l.activeRun - 1
	}
	for i := searchRuns; i >= 0; i -- {
		found, value = l.runs[i].LookUp(key)
		if found {
			return
		}
	}
	return
}

// 层满的时候要进行merge
func (l *DiskLevel) LevelFull() bool {
	return l.activeRun == l.numRuns
}

func (l *DiskLevel) LevelEmpty() bool {
	return l.activeRun == 0
}

