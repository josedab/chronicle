package compression

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// ZSTDCompressor provides high-performance Zstandard compression and decompression.
type ZSTDCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewZSTDCompressor creates a fast Zstandard compressor instance.
func NewZSTDCompressor() (*ZSTDCompressor, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, fmt.Errorf("failed to init zstd encoder: %w", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to init zstd decoder: %w", err)
	}
	return &ZSTDCompressor{
		encoder: enc,
		decoder: dec,
	}, nil
}

// Compress compresses raw byte data using Zstandard.
func (zc *ZSTDCompressor) Compress(src []byte) []byte {
	return zc.encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

// Decompress decompresses Zstandard encoded bytes.
func (zc *ZSTDCompressor) Decompress(src []byte) ([]byte, error) {
	return zc.decoder.DecodeAll(src, nil)
}

// CompressStream compresses from an io.Reader to an io.Writer using Zstandard stream.
func CompressZSTDStream(w io.Writer, r io.Reader) error {
	enc, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return err
	}
	defer enc.Close()

	_, err = io.Copy(enc, r)
	return err
}

// DecompressStream decompresses from an io.Reader to an io.Writer using Zstandard stream.
func DecompressZSTDStream(w io.Writer, r io.Reader) error {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer dec.Close()

	_, err = io.Copy(w, dec)
	return err
}

// FastCompressZSTD compresses a buffer with fast level.
func FastCompressZSTD(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	if _, err := enc.Write(src); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
