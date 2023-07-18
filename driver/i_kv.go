package driver

import (
	"context"

	"github.com/weedge/pkg/driver"
	openkvDriver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/xdis-storager/openkv"
)

type IKV interface {
	Get(key []byte) ([]byte, error)
	GetSlice(key []byte) (openkvDriver.ISlice, error)

	Put(key []byte, value []byte) error
	Delete(key []byte) error

	NewIterator() *openkv.Iterator

	NewWriteBatch() *openkv.WriteBatch

	RangeIterator(min []byte, max []byte, rangeType driver.RangeType) *openkv.RangeLimitIterator
	RevRangeIterator(min []byte, max []byte, rangeType driver.RangeType) *openkv.RangeLimitIterator
	RangeLimitIterator(min []byte, max []byte, rangeType driver.RangeType, offset int, count int) *openkv.RangeLimitIterator
	RevRangeLimitIterator(min []byte, max []byte, rangeType driver.RangeType, offset int, count int) *openkv.RangeLimitIterator

	Compact() error
	Close() error
}

type ICommitter interface {
	Commit(context.Context, *openkv.WriteBatch) error
}
