//go:build !windows

package cassandra

import (
	"errors"
	"os"
)

func syncHistoryNodeBackfillCheckpointDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
