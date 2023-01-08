package bitcask

type options struct {
	expirySeconds      int
	maxFileSize        uint64
	openTimeOutSeconds int
	readWrite          bool
	mergeSeconds       int
	checkSumCrc32      bool
	maxValueSize       uint64
}

func newOptions(expirySeconds int, maxFileSize uint64, openTimeOutSeconds int, readWrite bool, mergeSeconds int, checkSumCrc32 bool, maxValueSize uint64) *options {
	return &options{
		expirySeconds:      expirySeconds,
		maxFileSize:        maxFileSize,
		openTimeOutSeconds: openTimeOutSeconds,
		readWrite:          readWrite,
		mergeSeconds:       mergeSeconds,
		checkSumCrc32:      checkSumCrc32,
		maxValueSize:       maxValueSize}
}

func defaultOptions() *options {
	return &options{
		expirySeconds:      0,
		maxFileSize:        1 << 31,
		openTimeOutSeconds: 10,
		//readWrite:          readWrite,
		//mergeSeconds:       mergeSeconds,
		checkSumCrc32: false,
		maxValueSize:  1 << 20}
}
