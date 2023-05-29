package openkv

import (
	"bytes"

	driver "github.com/weedge/pkg/driver/openkv"
)

// Iterator wrap driver.IIterator, base iter
type Iterator struct {
	driver.IIterator
}

// Key returns a copy of key.
func (m *Iterator) Key() []byte {
	k := m.IIterator.Key()
	if k == nil {
		return nil
	}

	return append([]byte{}, k...)
}

// Value returns a copy of value.
func (m *Iterator) Value() []byte {
	v := m.IIterator.Value()
	if v == nil {
		return nil
	}

	return append([]byte{}, v...)
}

// RawKey returns a reference of key.
// you must be careful that it will be changed after next iterate.
func (m *Iterator) RawKey() []byte {
	return m.IIterator.Key()
}

// RawValue returns a reference of value.
// you must be careful that it will be changed after next iterate.
func (m *Iterator) RawValue() []byte {
	return m.IIterator.Value()
}

// BufKey copy key to b, if b len is small or nil, returns a new one.
func (m *Iterator) BufKey(b []byte) []byte {
	k := m.RawKey()
	if k == nil {
		return nil
	}
	if b == nil {
		b = []byte{}
	}

	b = b[0:0]
	return append(b, k...)
}

// BufValue copy value to b, if b len is small or nil, returns a new one.
func (m *Iterator) BufValue(b []byte) []byte {
	v := m.RawValue()
	if v == nil {
		return nil
	}

	if b == nil {
		b = []byte{}
	}

	b = b[0:0]
	return append(b, v...)
}

func (m *Iterator) Close() {
	if m.IIterator != nil {
		m.IIterator.Close()
		m.IIterator = nil
	}
}

func (m *Iterator) Valid() bool {
	return m.IIterator.Valid()
}

func (m *Iterator) Next() {
	m.IIterator.Next()
}

func (m *Iterator) Prev() {
	m.IIterator.Prev()
}

func (m *Iterator) SeekToFirst() {
	m.IIterator.First()
}

func (m *Iterator) SeekToLast() {
	m.IIterator.Last()
}

func (m *Iterator) Seek(key []byte) {
	m.IIterator.Seek(key)
}

// Find finds by key, if not found, nil returns.
func (m *Iterator) Find(key []byte) []byte {
	m.Seek(key)
	if m.Valid() {
		k := m.RawKey()
		if k == nil {
			return nil
		} else if bytes.Equal(k, key) {
			return m.Value()
		}
	}

	return nil
}

// RawFind finds by key, if not found, nil returns, else a reference of value returns.
// you must be careful that it will be changed after next iterate.
func (m *Iterator) RawFind(key []byte) []byte {
	m.Seek(key)
	if m.Valid() {
		k := m.RawKey()
		if k == nil {
			return nil
		} else if bytes.Equal(k, key) {
			return m.RawValue()
		}
	}

	return nil
}
