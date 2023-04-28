package lsm

import (
	"fmt"
	"math"
)

type DiskPart struct {
	DiskLevels []*DiskLevel // 磁盘有多层，需要做merge
	// NumDiskLevels    int
	diskRunsPerLevel int
	pageSize         int
	bfFp             float64
	mergedFrac       float64
}

func NewDiskPart(diskRunsPerLevel, pageSize, initRunSize int, mergedFrac float64, bfFp float64) (d *DiskPart) {
	// new disklevels
	d = &DiskPart{
		DiskLevels: make([]*DiskLevel, 0),
		// NumDiskLevels:    1,
		diskRunsPerLevel: diskRunsPerLevel,
		pageSize:         pageSize,
		bfFp:             bfFp,
		mergedFrac:       mergedFrac,
	}
	mergeSize := int(math.Ceil(float64(diskRunsPerLevel) * mergedFrac))
	diskLevel := NewDiskLevel(pageSize, 0, initRunSize, diskRunsPerLevel, mergeSize, bfFp) // 此处的level和DiskLevels的索引一致，这样方便理解，都是从0开始的

	d.DiskLevels = append(d.DiskLevels, diskLevel)
	return d
}

func (d *DiskPart) LookUp(key K) (found bool, value V) {
	for i := 0; i < len(d.DiskLevels); i++ {
		found, value = d.DiskLevels[i].LookUp(key)
		if found {
			return
		}
	}
	return
}

func (d *DiskPart) PrintElts() {
	for i := 0; i < len(d.DiskLevels); i++ {
		fmt.Printf("DISK LEVEL %d : \n", i)
		d.DiskLevels[i].PrintElts()
	}
}

func (d *DiskPart) Size() (size int) {
	size = 0
	for i := 0; i < len(d.DiskLevels); i++ {
		size += d.DiskLevels[i].NumElements()
	}
	return size
}

// 假设level = 1
func (d *DiskPart) MergeDiskRunsToLevel(level int) {
	// log.Printf("MergeDiskRunsToLevel:%d\n", level)
	isLast := false
	if level == len(d.DiskLevels)-1 {
		diskRunSize := d.DiskLevels[level].runSize * d.DiskLevels[level].mergeSize
		mergeSize := int(math.Ceil(float64(d.diskRunsPerLevel) * d.mergedFrac)) // 每层的mergesize是不是都一样。mergesize是一样，但是每层的大小runSize不一样。
		diskLevel := NewDiskLevel(d.pageSize, level+1, diskRunSize, d.diskRunsPerLevel, mergeSize, d.bfFp)
		d.DiskLevels = append(d.DiskLevels, diskLevel)
	}

	// 这个为啥判断的是下一层有没有满。而不是当前层有没有满？
	if d.DiskLevels[level+1].LevelFull() {
		// log.Printf("level:%d is full\n", level+1)
		d.MergeDiskRunsToLevel(level + 1)
	}

	// 确定是最后一层
	if level == len(d.DiskLevels)-2 && d.DiskLevels[level+1].LevelEmpty() {
		isLast = true // 刚创建了一层，确实是最后一层
	}
	runsToMerge := d.DiskLevels[level].GetRunsToMerge()
	// log.Printf("disk srclevel:%d GetRunsToMerge:%d dstLevel:%d\n", level, len(runsToMerge), level+1)
	d.DiskLevels[level+1].AddRuns(runsToMerge, isLast)
	d.DiskLevels[level].FreeMergedRuns(runsToMerge)
}
