// Code generated by mockery v2.29.0. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	openkv "github.com/weedge/pkg/driver/openkv"

	pkgdriver "github.com/weedge/pkg/driver"

	xdis_storageropenkv "github.com/weedge/xdis-storager/openkv"
)

// IKV is an autogenerated mock type for the IKV type
type IKV struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *IKV) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Compact provides a mock function with given fields:
func (_m *IKV) Compact() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: key
func (_m *IKV) Delete(key []byte) error {
	ret := _m.Called(key)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte) error); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Get provides a mock function with given fields: key
func (_m *IKV) Get(key []byte) ([]byte, error) {
	ret := _m.Called(key)

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) ([]byte, error)); ok {
		return rf(key)
	}
	if rf, ok := ret.Get(0).(func([]byte) []byte); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetSlice provides a mock function with given fields: key
func (_m *IKV) GetSlice(key []byte) (openkv.ISlice, error) {
	ret := _m.Called(key)

	var r0 openkv.ISlice
	var r1 error
	if rf, ok := ret.Get(0).(func([]byte) (openkv.ISlice, error)); ok {
		return rf(key)
	}
	if rf, ok := ret.Get(0).(func([]byte) openkv.ISlice); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(openkv.ISlice)
		}
	}

	if rf, ok := ret.Get(1).(func([]byte) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewIterator provides a mock function with given fields:
func (_m *IKV) NewIterator() *xdis_storageropenkv.Iterator {
	ret := _m.Called()

	var r0 *xdis_storageropenkv.Iterator
	if rf, ok := ret.Get(0).(func() *xdis_storageropenkv.Iterator); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.Iterator)
		}
	}

	return r0
}

// NewWriteBatch provides a mock function with given fields:
func (_m *IKV) NewWriteBatch() *xdis_storageropenkv.WriteBatch {
	ret := _m.Called()

	var r0 *xdis_storageropenkv.WriteBatch
	if rf, ok := ret.Get(0).(func() *xdis_storageropenkv.WriteBatch); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.WriteBatch)
		}
	}

	return r0
}

// Put provides a mock function with given fields: key, value
func (_m *IKV) Put(key []byte, value []byte) error {
	ret := _m.Called(key, value)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte, []byte) error); ok {
		r0 = rf(key, value)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RangeIterator provides a mock function with given fields: min, max, rangeType
func (_m *IKV) RangeIterator(min []byte, max []byte, rangeType pkgdriver.RangeType) *xdis_storageropenkv.RangeLimitIterator {
	ret := _m.Called(min, max, rangeType)

	var r0 *xdis_storageropenkv.RangeLimitIterator
	if rf, ok := ret.Get(0).(func([]byte, []byte, pkgdriver.RangeType) *xdis_storageropenkv.RangeLimitIterator); ok {
		r0 = rf(min, max, rangeType)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.RangeLimitIterator)
		}
	}

	return r0
}

// RangeLimitIterator provides a mock function with given fields: min, max, rangeType, offset, count
func (_m *IKV) RangeLimitIterator(min []byte, max []byte, rangeType pkgdriver.RangeType, offset int, count int) *xdis_storageropenkv.RangeLimitIterator {
	ret := _m.Called(min, max, rangeType, offset, count)

	var r0 *xdis_storageropenkv.RangeLimitIterator
	if rf, ok := ret.Get(0).(func([]byte, []byte, pkgdriver.RangeType, int, int) *xdis_storageropenkv.RangeLimitIterator); ok {
		r0 = rf(min, max, rangeType, offset, count)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.RangeLimitIterator)
		}
	}

	return r0
}

// RevRangeIterator provides a mock function with given fields: min, max, rangeType
func (_m *IKV) RevRangeIterator(min []byte, max []byte, rangeType pkgdriver.RangeType) *xdis_storageropenkv.RangeLimitIterator {
	ret := _m.Called(min, max, rangeType)

	var r0 *xdis_storageropenkv.RangeLimitIterator
	if rf, ok := ret.Get(0).(func([]byte, []byte, pkgdriver.RangeType) *xdis_storageropenkv.RangeLimitIterator); ok {
		r0 = rf(min, max, rangeType)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.RangeLimitIterator)
		}
	}

	return r0
}

// RevRangeLimitIterator provides a mock function with given fields: min, max, rangeType, offset, count
func (_m *IKV) RevRangeLimitIterator(min []byte, max []byte, rangeType pkgdriver.RangeType, offset int, count int) *xdis_storageropenkv.RangeLimitIterator {
	ret := _m.Called(min, max, rangeType, offset, count)

	var r0 *xdis_storageropenkv.RangeLimitIterator
	if rf, ok := ret.Get(0).(func([]byte, []byte, pkgdriver.RangeType, int, int) *xdis_storageropenkv.RangeLimitIterator); ok {
		r0 = rf(min, max, rangeType, offset, count)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*xdis_storageropenkv.RangeLimitIterator)
		}
	}

	return r0
}

type mockConstructorTestingTNewIKV interface {
	mock.TestingT
	Cleanup(func())
}

// NewIKV creates a new instance of IKV. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewIKV(t mockConstructorTestingTNewIKV) *IKV {
	mock := &IKV{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}