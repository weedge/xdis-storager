package storager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
	"github.com/weedge/xdis-storager/openkv"
)

// SnapshotHead is the head of a snapshot.
type SnapshotHead struct {
	CommitID uint64
}

// Read reads meta from the Reader.
func (h *SnapshotHead) Read(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, &h.CommitID)
}

// Write writes meta to the Writer
func (h *SnapshotHead) Write(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, h.CommitID)
}

// SaveSnapshotWithHead dumps data to the Writer with SnapshotHead
func (s *Storager) SaveSnapshotWithHead(ctx context.Context, h *SnapshotHead, w io.Writer) (err error) {
	var snap *openkv.Snapshot

	s.wLock.Lock()
	if snap, err = s.odb.NewSnapshot(); err != nil {
		s.wLock.Unlock()
		return err
	}
	s.wLock.Unlock()
	defer snap.Close()

	// new writeSize io buffer for io.Writer
	wb := bufio.NewWriterSize(w, 4096)
	if h != nil {
		if err = h.Write(wb); err != nil {
			return err
		}
	}

	it := snap.NewIterator()
	defer it.Close()
	it.SeekToFirst()

	compressBuf := make([]byte, 4096)
	var key []byte
	var value []byte
	for ; it.Valid(); it.Next() {
		key = it.RawKey()
		value = it.RawValue()

		// len(compress key) | compress key
		// key is a new slice from compressBuf substr or make a new slice
		key = snappy.Encode(compressBuf, key)
		if err = binary.Write(wb, binary.BigEndian, uint16(len(key))); err != nil {
			return err
		}
		if _, err = wb.Write(key); err != nil {
			return err
		}

		// len(compress value) | compress value
		// value is a new slice from compressBuf substr or make a new slice
		value = snappy.Encode(compressBuf, value)
		if err = binary.Write(wb, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}
		if _, err = wb.Write(value); err != nil {
			return err
		}
	}

	// write buffer flush to io.Writer
	return wb.Flush()
}

// RecoverFromSnapshotWithHead clears all data and loads dump file to db
// return snapshot head info,error
func (s *Storager) RecoverFromSnapshotWithHead(ctx context.Context, r io.Reader) (h *SnapshotHead, err error) {
	s.wLock.Lock()
	defer s.wLock.Unlock()

	// clear all data
	if err = s.flushAll(ctx); err != nil {
		return nil, err
	}

	// new io buffer for io.Reader
	rb := bufio.NewReaderSize(r, 4096)
	h = new(SnapshotHead)
	if err = h.Read(rb); err != nil {
		return nil, err
	}

	var keyLen uint16
	var keyBuf bytes.Buffer
	var valueLen uint32
	var valueBuf bytes.Buffer

	deKeyBuf := make([]byte, 4096)
	deValueBuf := make([]byte, 4096)
	var key, value []byte

	wb := s.odb.NewWriteBatch()
	defer wb.Close()
	n := 0
	for {
		// read key
		if err = binary.Read(rb, binary.BigEndian, &keyLen); err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}
		if _, err = io.CopyN(&keyBuf, rb, int64(keyLen)); err != nil {
			return nil, err
		}
		if key, err = snappy.Decode(deKeyBuf, keyBuf.Bytes()); err != nil {
			return nil, err
		}

		// read value
		if err = binary.Read(rb, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}
		if _, err = io.CopyN(&valueBuf, rb, int64(valueLen)); err != nil {
			return nil, err
		}
		if value, err = snappy.Decode(deValueBuf, valueBuf.Bytes()); err != nil {
			return nil, err
		}

		wb.Put(key, value)
		n++
		if n%1024 == 0 {
			if err = wb.Commit(); err != nil {
				return nil, err
			}
		}

		keyBuf.Reset()
		valueBuf.Reset()
	}

	if err = wb.Commit(); err != nil {
		return nil, err
	}

	return h, nil
}
