package lsm

type DiskPart struct {
	DiskLevels       []*DiskLevel // 磁盘有多层，需要做merge
	diskRunsPerLevel int
	pageSize         int
	bfFp             float64
}

func NewDiskPart(diskRunsPerLevel, pageSize, initRunSize, mergeSize int, bfFp float64) (d *DiskPart) {
	// new disklevels
	d = &DiskPart{
		DiskLevels:       make([]*DiskLevel, 0),
		diskRunsPerLevel: diskRunsPerLevel,
		pageSize:         pageSize,
		bfFp:             bfFp,
	}
	diskLevel := NewDiskLevel(pageSize, 1, initRunSize, diskRunsPerLevel, mergeSize, bfFp)
	d.DiskLevels = append(d.DiskLevels, diskLevel)
	return d
}
