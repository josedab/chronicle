package chronicle

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/chronicle-db/chronicle/internal/encoding"
)

type fileReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

const (
	headerMagic             = "CHRDB1"
	footerMagic             = "CHRFTR1"
	headerSize              = 16
	footerSize              = 24
	blockHeaderSize         = 9
	blockTypePartition byte = 1
	blockTypeIndex     byte = 2
)

func initStorage(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 0 {
		return nil
	}

	header := make([]byte, headerSize)
	copy(header, headerMagic)
	if _, err := file.Write(header); err != nil {
		return err
	}
	return file.Sync()
}

func persistPartition(file *os.File, part *Partition) error {
	payload, err := encodePartition(part)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(payload)

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	header := []byte{blockTypePartition}
	header = append(header, make([]byte, 8)...)
	binary.LittleEndian.PutUint32(header[1:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[5:], checksum)

	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}

	part.Offset = offset
	part.Length = int64(len(payload)) + blockHeaderSize
	part.Size = part.Length

	return nil
}

func persistIndex(file *os.File, idx *Index) error {
	payload, err := encodeIndex(idx)
	if err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(payload)

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	indexOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	header := []byte{blockTypeIndex}
	header = append(header, make([]byte, 8)...)
	binary.LittleEndian.PutUint32(header[1:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[5:], checksum)

	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}

	footer := make([]byte, footerSize)
	copy(footer, footerMagic)
	binary.LittleEndian.PutUint64(footer[8:], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[16:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(footer[20:], checksum)

	if _, err := file.Write(footer); err != nil {
		return err
	}

	return nil
}

func loadIndex(file *os.File) (*Index, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < footerSize {
		return newIndex(), nil
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, info.Size()-footerSize); err != nil {
		return nil, err
	}
	if string(footer[:len(footerMagic)]) != footerMagic {
		return newIndex(), nil
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[8:]))
	indexLength := int64(binary.LittleEndian.Uint32(footer[16:]))
	checksum := binary.LittleEndian.Uint32(footer[20:])

	if indexOffset <= 0 || indexLength <= 0 {
		return newIndex(), nil
	}

	header := make([]byte, blockHeaderSize)
	if _, err := file.ReadAt(header, indexOffset); err != nil {
		return nil, err
	}
	if header[0] != blockTypeIndex {
		return newIndex(), nil
	}
	headerLen := int64(binary.LittleEndian.Uint32(header[1:]))
	headerChecksum := binary.LittleEndian.Uint32(header[5:])
	if headerLen != indexLength {
		return newIndex(), nil
	}

	payload := make([]byte, indexLength)
	if _, err := file.ReadAt(payload, indexOffset+blockHeaderSize); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return newIndex(), nil
	}
	if checksum != headerChecksum {
		return newIndex(), nil
	}

	return decodeIndex(payload)
}

func encodeIndex(idx *Index) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	buf := &bytes.Buffer{}

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(idx.partitions))); err != nil {
		return nil, err
	}

	for _, part := range idx.partitions {
		if err := binary.Write(buf, binary.LittleEndian, part.ID); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.StartTime); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.EndTime); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.MinTime); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.MaxTime); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.PointCount); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, part.Length); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(idx.metrics))); err != nil {
		return nil, err
	}
	for metric := range idx.metrics {
		if err := encoding.WriteString(buf, metric); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, uint32(len(idx.seriesByID))); err != nil {
		return nil, err
	}
	for _, series := range idx.seriesByID {
		if err := binary.Write(buf, binary.LittleEndian, series.ID); err != nil {
			return nil, err
		}
		if err := encoding.WriteString(buf, series.Metric); err != nil {
			return nil, err
		}
		if err := encoding.WriteTags(buf, series.Tags); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeIndex(payload []byte) (*Index, error) {
	reader := bytes.NewReader(payload)
	idx := newIndex()

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	for i := uint32(0); i < count; i++ {
		part := &Partition{}
		if err := binary.Read(reader, binary.LittleEndian, &part.ID); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.StartTime); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.EndTime); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.MinTime); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.MaxTime); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.PointCount); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &part.Length); err != nil {
			return nil, err
		}

		part.Series = make(map[string]*SeriesData)
		part.loaded = false

		idx.byID[part.ID] = part
		idx.partitions = append(idx.partitions, part)
	}

	var metricCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &metricCount); err != nil {
		return nil, err
	}
	for i := uint32(0); i < metricCount; i++ {
		metric, err := encoding.ReadString(reader)
		if err != nil {
			return nil, err
		}
		if metric != "" {
			idx.metrics[metric] = struct{}{}
		}
	}

	var seriesCount uint32
	if err := binary.Read(reader, binary.LittleEndian, &seriesCount); err != nil {
		if err == io.EOF {
			idx.rebuildTimeIndexLocked()
			return idx, nil
		}
		return nil, err
	}
	for i := uint32(0); i < seriesCount; i++ {
		var seriesID uint64
		if err := binary.Read(reader, binary.LittleEndian, &seriesID); err != nil {
			return nil, err
		}
		metric, err := encoding.ReadString(reader)
		if err != nil {
			return nil, err
		}
		tags, err := encoding.ReadTags(reader)
		if err != nil {
			return nil, err
		}

		series := Series{ID: seriesID, Metric: metric, Tags: tags}
		idx.seriesByID[seriesID] = series
		idx.seriesByKey[seriesKey(metric, tags)] = series
		if seriesID > idx.nextSeriesID {
			idx.nextSeriesID = seriesID
		}

		metricSet := idx.metricSeries[metric]
		if metricSet == nil {
			metricSet = make(map[uint64]struct{})
			idx.metricSeries[metric] = metricSet
		}
		metricSet[seriesID] = struct{}{}

		for k, v := range tags {
			valueMap := idx.tagSeries[k]
			if valueMap == nil {
				valueMap = make(map[string]map[uint64]struct{})
				idx.tagSeries[k] = valueMap
			}
			set := valueMap[v]
			if set == nil {
				set = make(map[uint64]struct{})
				valueMap[v] = set
			}
			set[seriesID] = struct{}{}
		}
	}

	idx.rebuildTimeIndexLocked()

	return idx, nil
}
