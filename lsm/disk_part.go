package lsm

import "math"

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
	}
	mergeSize := int(math.Ceil(float64(diskRunsPerLevel) * mergedFrac))
	diskLevel := NewDiskLevel(pageSize, 1, initRunSize, diskRunsPerLevel, mergeSize, bfFp)
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

func (d *DiskPart) MergeDiskRunsToLevel(level int) {
	isLast := false
	if level == len(d.DiskLevels) {
		initRunSize := d.DiskLevels[level-1].runSize * d.DiskLevels[level-1].mergeSize
		mergeSize := int(math.Ceil(float64(d.diskRunsPerLevel) * d.mergedFrac)) // 每层的mergesize是不是都一样
		diskLevel := NewDiskLevel(d.pageSize, level+1, initRunSize, d.diskRunsPerLevel, mergeSize, d.bfFp)
		d.DiskLevels = append(d.DiskLevels, diskLevel)
	}

	if d.DiskLevels[level].LevelFull() {
		d.MergeDiskRunsToLevel(level + 1)
	}

	if level == len(d.DiskLevels)-1 && d.DiskLevels[level].LevelEmpty() {
		isLast = true
	}
	runsToMerge := d.DiskLevels[level-1].GetRunsToMerge()
	d.DiskLevels[level].AddRuns(runsToMerge, isLast)
	d.DiskLevels[level].FreeMergedRuns(runsToMerge)

}
