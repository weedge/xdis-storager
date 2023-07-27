package storager

import (
	"testing"

	"github.com/weedge/pkg/driver"
)

func TestDB_Implements(t *testing.T) {
	//b1 := encodeDbIndexKey(10238, CodeTypeMeta)
	//e1 := b1[len(b1)-1] + 1
	//b2 := encodeDbIndexKey(10239, CodeTypeMeta)
	//e2 := b2[len(b2)-1] + 1
	//println(b1, e1, b2, e2)
	var i interface{} = &DB{}
	if _, ok := i.(driver.IDB); !ok {
		t.Fatalf("does not implement driver.IDB")
	}
}

func TestDBSlots_Implements(t *testing.T) {
	var i interface{} = &DB{}
	if _, ok := i.(driver.IDBSlots); !ok {
		t.Fatalf("does not implement driver.IDBSlots")
	}
}
