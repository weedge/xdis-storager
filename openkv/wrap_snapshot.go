package openkv

import (
	driver "github.com/weedge/pkg/driver/openkv"
)

// Snapshot wrap driver.ISnapshot interface op
type Snapshot struct {
	driver.ISnapshot
}

func (s *Snapshot) NewIterator() *Iterator {
	it := &Iterator{
		IIterator: s.ISnapshot.NewIterator(),
	}

	return it
}

func (s *Snapshot) Get(key []byte) ([]byte, error) {
	v, err := s.ISnapshot.Get(key)
	return v, err
}

func (s *Snapshot) Close() {
	if s.ISnapshot != nil {
		s.ISnapshot.Close()
		s.ISnapshot = nil
	}
}
