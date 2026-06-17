package fileutil

import (
	"os"
	"path/filepath"
)

type DirectoryStats struct {
	Files uint64
	Bytes uint64
}

func StatDir(root string) (DirectoryStats, error) {
	var stats DirectoryStats
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		stats.Files++
		if info.Size() > 0 {
			stats.Bytes += uint64(info.Size())
		}
		return nil
	})
	return stats, err
}
