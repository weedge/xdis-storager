package openkv

import "bytes"

const (
	IteratorForward  uint8 = 0
	IteratorBackward uint8 = 1
)

const (
	RangeClose uint8 = 0x00
	RangeLOpen uint8 = 0x01
	RangeROpen uint8 = 0x10
	RangeOpen  uint8 = 0x11
)

// Range min must less or equal than max
//
// range type:
//
//	close: [min, max]
//	open: (min, max)
//	lopen: (min, max]
//	ropen: [min, max)
type Range struct {
	Min []byte
	Max []byte

	Type uint8
}

type Limit struct {
	Offset int
	Count  int
}

type RangeLimitIterator struct {
	*Iterator

	r *Range
	l *Limit

	step int

	//0 for IteratorForward, 1 for IteratorBackward
	direction uint8
}

func (it *RangeLimitIterator) Valid() bool {
	if it.l.Offset < 0 {
		return false
	}
	if !it.Iterator.Valid() {
		return false
	}
	if it.l.Count >= 0 && it.step >= it.l.Count {
		return false
	}

	if it.direction == IteratorForward && it.r.Max != nil {
		r := bytes.Compare(it.RawKey(), it.r.Max)
		if it.r.Type&RangeROpen > 0 {
			return !(r >= 0)
		}
		return !(r > 0)
	}

	if it.direction != IteratorForward && it.r.Min != nil {
		r := bytes.Compare(it.RawKey(), it.r.Min)
		if it.r.Type&RangeLOpen > 0 {
			return !(r <= 0)
		}
		return !(r < 0)
	}

	return true
}

func (it *RangeLimitIterator) Next() {
	it.step++

	if it.direction == IteratorForward {
		it.Iterator.Next()
	} else {
		it.Iterator.Prev()
	}
}

func NewRangeLimitIterator(i *Iterator, r *Range, l *Limit) *RangeLimitIterator {
	return rangeLimitIterator(i, r, l, IteratorForward)
}

func NewRevRangeLimitIterator(i *Iterator, r *Range, l *Limit) *RangeLimitIterator {
	return rangeLimitIterator(i, r, l, IteratorBackward)
}

func NewRangeIterator(i *Iterator, r *Range) *RangeLimitIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, IteratorForward)
}

func NewRevRangeIterator(i *Iterator, r *Range) *RangeLimitIterator {
	return rangeLimitIterator(i, r, &Limit{0, -1}, IteratorBackward)
}

func rangeLimitIterator(i *Iterator, r *Range, l *Limit, direction uint8) *RangeLimitIterator {
	it := &RangeLimitIterator{
		Iterator:  i,
		r:         r,
		l:         l,
		direction: direction,
		step:      0,
	}
	if l.Offset < 0 {
		return it
	}

	if direction == IteratorForward {
		if r.Min == nil {
			i.SeekToFirst()
		} else {
			i.Seek(r.Min)
			if r.Type&RangeLOpen > 0 {
				if i.Valid() && bytes.Equal(i.RawKey(), r.Min) {
					i.Next()
				}
			}
		}
	} else {
		if r.Max == nil {
			i.SeekToLast()
		} else {
			i.Seek(r.Max)
			if !i.Valid() {
				i.SeekToLast()
			} else {
				if !bytes.Equal(i.RawKey(), r.Max) {
					i.Prev()
				}
			}

			if r.Type&RangeROpen > 0 {
				if i.Valid() && bytes.Equal(i.RawKey(), r.Max) {
					i.Prev()
				}
			}
		}
	}

	for idx := 0; idx < l.Offset; idx++ {
		if i.Valid() {
			if it.direction == IteratorForward {
				i.Next()
			} else {
				i.Prev()
			}
		}
	}

	return it
}
