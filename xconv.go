package storager

import (
	"reflect"
	"strconv"
	"unsafe"
)

// no copy to change slice to string
// use your own risk
func Bytes2String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// no copy to change string to slice
// use your own risk
func String2Bytes(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

// StrInt64 gets the 64 integer with string format.
func StrInt64(v []byte, err error) (int64, error) {
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	} else {
		return strconv.ParseInt(Bytes2String(v), 10, 64)
	}
}

// StrUint64 gets the unsigned 64 integer with string format.
func StrUint64(v []byte, err error) (uint64, error) {
	res, err := StrInt64(v, err)
	return uint64(res), err
}

// StrInt32 gets the 32 integer with string format.
func StrInt32(v []byte, err error) (int32, error) {
	res, err := StrInt64(v, err)
	return int32(res), err
}

// StrInt8 ets the 8 integer with string format.
func StrInt8(v []byte, err error) (int8, error) {
	res, err := StrInt64(v, err)
	return int8(res), err
}
