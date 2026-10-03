//go:build windows

package chronicle

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// diskUsage returns free and total bytes for the filesystem containing path.
func diskUsage(path string) (free uint64, total uint64, err error) {
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return 0, 0, err
	}
	dirUTF16, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, 0, err
	}
	var totalFree uint64
	if err := windows.GetDiskFreeSpaceEx(dirUTF16, &free, &total, &totalFree); err != nil {
		return 0, 0, err
	}
	return free, total, nil
}
