package lsm

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
)

type LSM struct {
	MemData   *MemPart // 内存只有一层
	DiskData  *DiskPart
	mergeLock *sync.Mutex
	mergeChan chan bool
	// 这个地方还要加锁, 用于merge, 保证线程安全
}

// diskRun的大小取决于合并的大小
func NewLSM(eltsPerRun int, numRuns int, mergedFrac float64, bfFp float64, pageSize int, diskRunsPerLevel int) *LSM {
	// 初始化内存部分
	numToMerge := int(math.Ceil(mergedFrac * float64(numRuns)))
	initRunSize := eltsPerRun * numToMerge
	log.Printf("init lsm\n")
	return &LSM{
		MemData:   NewMemPart(eltsPerRun, numRuns, mergedFrac, bfFp),
		DiskData:  NewDiskPart(diskRunsPerLevel, pageSize, initRunSize, mergedFrac, bfFp),
		mergeLock: &sync.Mutex{},
	}
}

// 先插入到内存，如果内存run满了 要sink到磁盘，磁盘是递归下沉
func (l *LSM) InsertKey(key K, value V) {
	log.Printf("insert key:%v value:%v\n", key, value)
	if l.MemData.IsFull() {
		log.Println("memedata is full, start mergeing")
		l.DoMerge()
	}
	l.MemData.InsertKey(key, value)

}

func (l *LSM) Lookup(key K) (found bool, value V) {
	found, value = l.MemData.LookUp(key)
	if found {
		return value != V_TOMBSTONE, value
	}

	// [todo]make sure that there isn't a merge happening as you search the disk, 这个地方要加一个merge的锁的等待
	if l.mergeChan != nil {
		<-l.mergeChan
	}

	found, value = l.DiskData.LookUp(key)
	if found {
		return value != V_TOMBSTONE, value
	}
	return
}

func (l *LSM) DeleteKey(key K) {
	l.InsertKey(key, V_TOMBSTONE)
}

func (l *LSM) PrintElts() {
	if l.mergeChan != nil {
		<-l.mergeChan
	}

	fmt.Println("MEMORY BUFFER")
	l.MemData.PrintElts()

	fmt.Println("DISK BUFFER")
	l.DiskData.PrintElts()
}

// 输出每层的个数
// [easy]然后PrintElts
func (l *LSM) PrintStats() {
	// fmt.Printf("Number of Elements: %d\n", total) // 这个要根据range去计算。从最小到最大遍历剔除删除的，重复的
	fmt.Printf("Number of Elements in Disk Buffer (including deletes): %d\n", l.DiskData.Size())
	fmt.Printf("Number of Elements in Memory Buffer (including deletes):  %d\n", l.MemData.Size())
	fmt.Println("KEY VALUE DUMP BY LEVEL: ")
	l.PrintElts()
}

// [middle]磁盘往下合并

// [middle]将要merge的数据写入到disk --- 这个是memrun的需求
// bfToMerge 这个都没有写进去，感觉这个可以去掉。估计bloom是重新计算
func (l *LSM) MergeMemRuns(runsToMerge []Run) {
	capacity := l.MemData.EltsPerRun * l.MemData.NumToMerge
	toMerge := make([]KVPair, 0)
	for i := 0; i < len(runsToMerge); i++ {
		toMerge = append(toMerge, (runsToMerge[i]).GetAll()...)
	}
	// l.MemData.FreeMergedRuns(runsToMerge, bfToMerge)
	sort.Slice(toMerge, func(i, j int) bool {
		return lessThan(toMerge[i].Key, toMerge[j].Key)
	})
	log.Printf("MergeMemRuns toMerge:%v capacity:%d\n", toMerge, capacity)
	l.mergeLock.Lock()
	// 当层磁盘满了要下沉到下一层
	if l.DiskData.DiskLevels[0].LevelFull() {
		l.DiskData.MergeDiskRunsToLevel(0) // 这个是递归的
	}
	l.DiskData.DiskLevels[0].AddRunByArray(toMerge, capacity)
	l.mergeLock.Unlock()
}

// [middle]
// 【问题】你不merge完，我怎么往里面写
func (l *LSM) DoMerge() {
	if l.MemData.NumToMerge == 0 {
		return
	}
	runsToMerge := l.MemData.GetRunsToMerge()
	log.Printf("runsToMerge:%d \n", len(runsToMerge))
	if l.mergeChan != nil {
		<-l.mergeChan
	}
	l.mergeChan = make(chan bool)
	go func() {
		l.MergeMemRuns(runsToMerge)
		close(l.mergeChan)
	}()

	// go l.MergeMemRuns(runsToMerge) // 【todo】此处可以加一个channel往channel里面写，或者使用sync waitgroup，要监控这个协程的处理结束，要使用channel进行通信
	// todo: 要主动释放空间 numtoMerge

	l.MemData.ResetMergedRuns()
}
