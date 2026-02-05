package gpucompression

import chronicle "github.com/chronicle-db/chronicle"

// Add adds data to the batch buffer.
func (bc *BatchCompressor) Add(data []byte) {
	bc.bufferMu.Lock()
	bc.buffer = append(bc.buffer, data)
	bc.bufferMu.Unlock()
}

// Flush compresses all buffered data.
func (bc *BatchCompressor) Flush(codec chronicle.CodecType) ([][]byte, error) {
	bc.bufferMu.Lock()
	batch := bc.buffer
	bc.buffer = make([][]byte, 0, bc.batchSize)
	bc.bufferMu.Unlock()

	if len(batch) == 0 {
		return nil, nil
	}

	return bc.gc.CompressBatch(batch, codec)
}
