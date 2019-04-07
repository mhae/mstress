package diskspace

import "syscall"
import "os"

// Get space in bytes for current directory
func CurrentWd() uint64 {
	var stat syscall.Statfs_t

	wd, _ := os.Getwd()

	syscall.Statfs(wd, &stat)

	// Available blocks * size per block = available space in bytes
	return stat.Bavail * uint64(stat.Bsize)
}

// Get space in bytes for current directory
func Dir(path string) uint64 {
	var stat syscall.Statfs_t

	syscall.Statfs(path, &stat)
	// Available blocks * size per block = available space in bytes
	return stat.Bavail * uint64(stat.Bsize)
}
