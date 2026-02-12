package gpucompression

// Write writes data to the streaming compressor.
func (sc *StreamingCompressor) Write(data []byte) (int, error) {
	sc.bufferMu.Lock()
	defer sc.bufferMu.Unlock()

	n, err := sc.buffer.Write(data)
	if err != nil {
		return n, err
	}

	if sc.buffer.Len() >= sc.window {
		chunk := make([]byte, sc.window)
		sc.buffer.Read(chunk)

		compressed, err := sc.gc.Compress(chunk, sc.codec)
		if err != nil {
			return n, err
		}

		select {
		case sc.output <- compressed:
		default:

		}
	}

	return n, nil
}

// Output returns the compressed output channel.
func (sc *StreamingCompressor) Output() <-chan []byte {
	return sc.output
}

// Flush flushes remaining data.
func (sc *StreamingCompressor) Flush() ([]byte, error) {
	sc.bufferMu.Lock()
	defer sc.bufferMu.Unlock()

	if sc.buffer.Len() == 0 {
		return nil, nil
	}

	data := sc.buffer.Bytes()
	sc.buffer.Reset()

	return sc.gc.Compress(data, sc.codec)
}

// Close closes the streaming compressor.
func (sc *StreamingCompressor) Close() error {
	close(sc.output)
	return nil
}
