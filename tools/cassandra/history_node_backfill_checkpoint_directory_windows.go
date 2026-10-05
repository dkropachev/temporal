package cassandra

func syncHistoryNodeBackfillCheckpointDirectory(string) error {
	// FlushFileBuffers cannot flush the read-only directory handle returned by os.Open.
	return nil
}
