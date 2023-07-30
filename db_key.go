package storager

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/weedge/pkg/utils"
)

// buf change
func binaryUvarint(buf []byte) (uint64, int, error) {
	data, n := binary.Uvarint(buf)
	if n == 0 {
		return data, n, fmt.Errorf("buf too small")
	}
	if n < 0 {
		return data, n, fmt.Errorf("value larger than 64 bits (overflow)")
	}
	return data, n, nil
}

// | uvarint DBIndex |
func encodeIndex(index int) []byte {
	buf := make([]byte, MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(index))
	return buf[:n]
}

// | Version | CodeType | uvarint DBIndex |
func encodeDbIndexKey(index int, codeType byte) []byte {
	return utils.ConcatBytes([][]byte{{Version, codeType}, encodeIndex(index)})
}

// | Version | CodeType | uvarint DBIndex (last byte + 1) |
func encodeDbIndexEndKey(index int, codeType byte) []byte {
	ek := utils.ConcatBytes([][]byte{{Version, codeType}, encodeIndex(index)})
	ek[len(ek)-1] += 1
	return ek
}

// | Version | CodeType | uvarint DBIndex |
func (db *DB) encodeDbIndex(codeType byte) []byte {
	return encodeDbIndexKey(db.index, codeType)
}

// | Version | CodeTypeMeta | uvarint DBIndex |
// | Version | CodeTypeMeta | uvarint DBIndex | uvarint Slot |
func (db *DB) encodeDbIndexSlot(slot uint64) []byte {
	if db.store.opts.Slots <= 0 {
		return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf})
	}

	slotBuf := make([]byte, MaxVarintLen64)
	n := binary.PutUvarint(slotBuf, slot)
	return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf, slotBuf[0:n]})
}

// | Version | CodeTypeMeta | uvarint DBIndex |
// | Version | CodeTypeMeta | uvarint DBIndex | uvarint Slot | tagLen | Tag |
func (db *DB) encodeDbIndexSlotTag(tag []byte) []byte {
	if db.store.opts.Slots <= 0 {
		return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf})
	}

	slot := db.slot.HashTagToSlot(tag)
	slotBuf := make([]byte, MaxVarintLen64)
	n := binary.PutUvarint(slotBuf, uint64(slot))

	tagLenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(tagLenBuf, uint16(len(tag)))
	return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf, slotBuf[0:n], tagLenBuf, tag})
}

// encodeDbIndexSlotTagByKey encode key by slot,hash of the key
// | Version | CodeTypeMeta | uvarint DBIndex | dataType | Key |
// | Version | CodeTypeMeta | uvarint DBIndex | uvarint Slot | tagLen | Tag | dataType | Key |
func (db *DB) encodeDbIndexSlotTagKey(key []byte, dataType byte) []byte {
	if db.store.opts.Slots <= 0 {
		return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf, {dataType}, key})
	}

	tag, slot := db.slot.HashKeyToSlot(key)
	slotBuf := make([]byte, MaxVarintLen64)
	n := binary.PutUvarint(slotBuf, uint64(slot))

	// need use varint-encoded to compress key, if tagLen is big
	tagLenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(tagLenBuf, uint16(len(tag)))
	return utils.ConcatBytes([][]byte{{Version, CodeTypeMeta}, db.indexVarBuf, slotBuf[0:n], tagLenBuf, tag, {dataType}, key})
}

type MetaObjKey struct {
	Version  byte
	CodeType byte
	DBIndex  uint64
	SlotId   uint64
	Tag      []byte
	DataType byte
	DataKey  []byte
}

func decodeDbIndexSlotTagMetaKey(index int, ek []byte) (meta *MetaObjKey, err error) {
	pos := 0
	meta = new(MetaObjKey)
	if len(ek) < 2 {
		err = fmt.Errorf("key is too small")
		return
	}
	meta.Version = ek[0]
	meta.CodeType = ek[1]

	// DB Index
	pos += 2
	data, n, err := binaryUvarint(ek[pos:])
	if err != nil {
		return
	}
	meta.DBIndex = data
	if gOpts.Slots <= 0 {
		return
	}

	// SlotId
	pos += n
	data, n, err = binaryUvarint(ek[pos:])
	if err != nil {
		return
	}
	meta.SlotId = data

	// Tag
	pos += n
	if pos == len(ek) {
		return
	}
	if pos > len(ek) {
		err = fmt.Errorf("overflow pos:%d > len(ek):%d", pos, len(ek))
		return
	}
	tagLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tagLen == 0 {
		return
	}
	meta.Tag = ek[pos : pos+tagLen]

	// dataType
	pos += tagLen
	meta.DataType = ek[pos]

	// Key
	pos++
	meta.DataKey = ek[pos:]

	return
}

func (db *DB) decodeDbIndexSlotTagMetaKey(ek []byte) (meta *MetaObjKey, err error) {
	return decodeDbIndexSlotTagMetaKey(db.index, ek)
}

func (db *DB) checkDbIndexSlotTagEncodeKey(ek []byte) (pos int, err error) {
	if pos, err = db.checkDbIndex(ek); err != nil {
		return
	}
	if db.store.opts.Slots <= 0 {
		return
	}

	_, n, err := binaryUvarint(ek[pos:])
	if err != nil {
		return 0, err
	}
	pos += n
	if pos == len(ek) {
		return
	}
	if pos > len(ek) {
		return 0, fmt.Errorf("overflow pos:%d > len(ek):%d", pos, len(ek))
	}
	tagLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if tagLen == 0 {
		return
	}
	pos += tagLen

	return
}

func checkDbIndex(index int, ek []byte) (int, error) {
	indexVarBuf := encodeIndex(index)
	if len(ek) < len(indexVarBuf)+2 {
		return 0, fmt.Errorf("key is too small")
	}
	if !bytes.Equal(indexVarBuf, ek[2:2+len(indexVarBuf)]) {
		return 0, fmt.Errorf("invalid db index")
	}

	return 2 + len(indexVarBuf), nil
}

func (db *DB) checkDbIndex(ek []byte) (int, error) {
	return checkDbIndex(db.index, ek)
}

// --- string ---

// | Version | CodeTypeData | uvarint DBIndex | StringType | Key |
func (db *DB) encodeStringKey(key []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	ek := make([]byte, len(key)+1+len(gBuf))
	pos := copy(ek, gBuf)
	ek[pos] = StringType
	pos++
	copy(ek[pos:], key)
	return ek
}

func (db *DB) decodeStringKey(ek []byte) ([]byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, err
	}
	if pos+1 > len(ek) || ek[pos] != StringType {
		return nil, ErrStringKey
	}

	pos++

	return ek[pos:], nil
}

// --- list ---

// | Version | CodeTypeData | uvarint DBIndex | LMetaType | Key |
func (db *DB) lEncodeMetaKey(key []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+1+len(gBuf))
	pos := copy(buf, gBuf)
	buf[pos] = LMetaType
	pos++

	copy(buf[pos:], key)
	return buf
}

func (db *DB) lDecodeMetaKey(ek []byte) ([]byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != LMetaType {
		return nil, ErrLMetaKey
	}

	pos++
	return ek[pos:], nil
}

// | Version | CodeTypeData | uvarint DBIndex | ListType | lenKey | Key | seq |
func (db *DB) lEncodeListKey(key []byte, seq int32) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+7+len(gBuf))

	pos := copy(buf, gBuf)

	buf[pos] = ListType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	binary.BigEndian.PutUint32(buf[pos:], uint32(seq))

	return buf
}

func (db *DB) lDecodeListKey(ek []byte) (key []byte, seq int32, err error) {
	pos := 0
	pos, err = db.checkDbIndex(ek)
	if err != nil {
		return
	}

	if pos+1 > len(ek) || ek[pos] != ListType {
		err = ErrListKey
		return
	}

	pos++

	if pos+2 > len(ek) {
		err = ErrListKey
		return
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2
	if keyLen+pos+4 != len(ek) {
		err = ErrListKey
		return
	}

	key = ek[pos : pos+keyLen]
	seq = int32(binary.BigEndian.Uint32(ek[pos+keyLen:]))
	return
}

// --- hash ---

// | Version | CodeTypeData | uvarint DBIndex | HSizeType |  Key |
func (db *DB) hEncodeSizeKey(key []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+1+len(gBuf))

	pos := 0
	n := copy(buf, gBuf)

	pos += n
	buf[pos] = HSizeType

	pos++
	copy(buf[pos:], key)

	return buf
}

func (db *DB) hDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != HSizeType {
		return nil, ErrHSizeKey
	}
	pos++

	return ek[pos:], nil
}

// | Version | CodeTypeData | uvarint DBIndex | HashType | lenKey | Key | hashStartSep | field |
func (db *DB) hEncodeHashKey(key []byte, field []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+len(field)+1+1+2+len(gBuf))

	pos := 0
	n := copy(buf, gBuf)
	pos += n

	buf[pos] = HashType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = hashStartSep
	pos++
	copy(buf[pos:], field)

	return buf
}

func (db *DB) hDecodeHashKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != HashType {
		return nil, nil, ErrHashKey
	}
	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrHashKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, ErrHashKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, ErrHashKey
	}

	pos++
	field := ek[pos:]
	return key, field, nil
}

// | Version | CodeTypeData | uvarint DBIndex | HashType | lenKey | Key | hashStartSep | field |
func (db *DB) hEncodeStartKey(key []byte) []byte {
	return db.hEncodeHashKey(key, nil)
}

// | Version | CodeTypeData | uvarint DBIndex | HashType | lenKey | Key | hashStartSep+1 | field |
func (db *DB) hEncodeStopKey(key []byte) []byte {
	k := db.hEncodeHashKey(key, nil)

	k[len(k)-1] = hashStopSep

	return k
}

// --- set ---

// | Version | CodeTypeData | uvarint DBIndex | SSizeType |  Key |
func (db *DB) sEncodeSizeKey(key []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+1+len(gBuf))

	pos := copy(buf, gBuf)
	buf[pos] = SSizeType

	pos++

	copy(buf[pos:], key)
	return buf
}

func (db *DB) sDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != SSizeType {
		return nil, ErrSSizeKey
	}
	pos++

	return ek[pos:], nil
}

// | Version | CodeTypeData | uvarint DBIndex | SetType | keyLen | Key | setStartSep | member |
func (db *DB) sEncodeSetKey(key []byte, member []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+len(member)+1+1+2+len(gBuf))

	pos := copy(buf, gBuf)

	buf[pos] = SetType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = setStartSep
	pos++
	copy(buf[pos:], member)

	return buf
}

func (db *DB) sDecodeSetKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != SetType {
		return nil, nil, ErrSetKey
	}

	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		return nil, nil, ErrSetKey
	}

	key := ek[pos : pos+keyLen]
	pos += keyLen

	if ek[pos] != hashStartSep {
		return nil, nil, ErrSetKey
	}

	pos++
	member := ek[pos:]
	return key, member, nil
}

func (db *DB) sEncodeStartKey(key []byte) []byte {
	return db.sEncodeSetKey(key, nil)
}

func (db *DB) sEncodeStopKey(key []byte) []byte {
	k := db.sEncodeSetKey(key, nil)

	k[len(k)-1] = setStopSep

	return k
}

// --- zset ---

// | Version | CodeTypeData | uvarint DBIndex | ZSizeType | Key |
func (db *DB) zEncodeSizeKey(key []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+1+len(gBuf))
	pos := copy(buf, gBuf)
	buf[pos] = ZSizeType
	pos++
	copy(buf[pos:], key)
	return buf
}

func (db *DB) zDecodeSizeKey(ek []byte) ([]byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, err
	}

	if pos+1 > len(ek) || ek[pos] != ZSizeType {
		return nil, ErrZSizeKey
	}
	pos++
	return ek[pos:], nil
}

// | Version | CodeTypeData | uvarint DBIndex | ZSetType | keyLen | Key | zsetStartMemSep | member |
func (db *DB) zEncodeSetKey(key []byte, member []byte) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+len(member)+4+len(gBuf))

	pos := copy(buf, gBuf)

	buf[pos] = ZSetType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)

	return buf
}

func (db *DB) zDecodeSetKey(ek []byte) ([]byte, []byte, error) {
	pos, err := db.checkDbIndex(ek)
	if err != nil {
		return nil, nil, err
	}

	if pos+1 > len(ek) || ek[pos] != ZSetType {
		return nil, nil, ErrZSetKey
	}

	pos++

	if pos+2 > len(ek) {
		return nil, nil, ErrZSetKey
	}

	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	if keyLen+pos > len(ek) {
		return nil, nil, ErrZSetKey
	}

	pos += 2
	key := ek[pos : pos+keyLen]

	if ek[pos+keyLen] != zsetStartMemSep {
		return nil, nil, ErrZSetKey
	}
	pos++

	member := ek[pos+keyLen:]
	return key, member, nil
}

// for mem score
// | Version | CodeTypeData | uvarint DBIndex | ZSetType | keyLen | Key | zsetStartMemSep | member |
func (db *DB) zEncodeStartSetKey(key []byte) []byte {
	k := db.zEncodeSetKey(key, nil)
	return k
}

// | Version | CodeTypeData | uvarint DBIndex | ZSetType | keyLen | Key | zsetStartMemSep+1 | member |
func (db *DB) zEncodeStopSetKey(key []byte) []byte {
	k := db.zEncodeSetKey(key, nil)
	k[len(k)-1] = zsetStopMemSep
	return k
}

// for score range
// | Version | CodeTypeData | uvarint DBIndex | ZScoreType | keyLen | Key | zsetNScoreSep,zsetPScoreSep | score | zsetStartMemSep | member |
func (db *DB) zEncodeScoreKey(key []byte, member []byte, score int64) []byte {
	gBuf := db.encodeDbIndex(CodeTypeData)
	buf := make([]byte, len(key)+len(member)+13+len(gBuf))

	pos := copy(buf, gBuf)

	buf[pos] = ZScoreType
	pos++

	binary.BigEndian.PutUint16(buf[pos:], uint16(len(key)))
	pos += 2

	copy(buf[pos:], key)
	pos += len(key)

	if score < 0 {
		buf[pos] = zsetNScoreSep
	} else {
		buf[pos] = zsetPScoreSep
	}

	pos++
	binary.BigEndian.PutUint64(buf[pos:], uint64(score))
	pos += 8

	buf[pos] = zsetStartMemSep
	pos++

	copy(buf[pos:], member)
	return buf
}

// | Version | CodeTypeData | uvarint DBIndex | ZScoreType | keyLen | Key | zsetNScoreSep,zsetPScoreSep | score | zsetStartMemSep | member |
func (db *DB) zEncodeStartScoreKey(key []byte, score int64) []byte {
	return db.zEncodeScoreKey(key, nil, score)
}

// | Version | CodeTypeData | uvarint DBIndex | ZScoreType | keyLen | Key | zsetNScoreSep,zsetPScoreSep | score | zsetStartMemSep+1 | member |
func (db *DB) zEncodeStopScoreKey(key []byte, score int64) []byte {
	k := db.zEncodeScoreKey(key, nil, score)
	k[len(k)-1] = zsetStopMemSep
	return k
}

func (db *DB) zDecodeScoreKey(ek []byte) (key []byte, member []byte, score int64, err error) {
	pos := 0
	pos, err = db.checkDbIndex(ek)
	if err != nil {
		return
	}

	if pos+1 > len(ek) || ek[pos] != ZScoreType {
		err = ErrZScoreKey
		return
	}
	pos++

	if pos+2 > len(ek) {
		err = ErrZScoreKey
		return
	}
	keyLen := int(binary.BigEndian.Uint16(ek[pos:]))
	pos += 2

	if keyLen+pos > len(ek) {
		err = ErrZScoreKey
		return
	}

	key = ek[pos : pos+keyLen]
	pos += keyLen

	if pos+10 > len(ek) {
		err = ErrZScoreKey
		return
	}

	if (ek[pos] != zsetNScoreSep) && (ek[pos] != zsetPScoreSep) {
		err = ErrZScoreKey
		return
	}
	pos++

	score = int64(binary.BigEndian.Uint64(ek[pos:]))
	pos += 8

	if ek[pos] != zsetStartMemSep {
		err = ErrZScoreKey
		return
	}

	pos++

	member = ek[pos:]
	return
}

// --- scan key ---

func (db *DB) encodeScanMinKey(storeDataType byte, key []byte) ([]byte, error) {
	return db.encodeScanKey(storeDataType, key)
}

func (db *DB) encodeScanMaxKey(storeDataType byte, key []byte) ([]byte, error) {
	if len(key) > 0 {
		return db.encodeScanKey(storeDataType, key)
	}

	k, err := db.encodeScanKey(storeDataType, nil)
	if err != nil {
		return nil, err
	}
	k[len(k)-1] = storeDataType + 1
	return k, nil
}

func (db *DB) encodeScanKey(storeDataType byte, key []byte) ([]byte, error) {
	switch storeDataType {
	case StringType:
		return db.encodeStringKey(key), nil
	case LMetaType:
		return db.lEncodeMetaKey(key), nil
	case HSizeType:
		return db.hEncodeSizeKey(key), nil
	case ZSizeType:
		return db.zEncodeSizeKey(key), nil
	case SSizeType:
		return db.sEncodeSizeKey(key), nil
	default:
		return nil, ErrDataType
	}
}

func (db *DB) decodeScanKey(storeDataType byte, ek []byte) (key []byte, err error) {
	switch storeDataType {
	case StringType:
		key, err = db.decodeStringKey(ek)
	case LMetaType:
		key, err = db.lDecodeMetaKey(ek)
	case HSizeType:
		key, err = db.hDecodeSizeKey(ek)
	case ZSizeType:
		key, err = db.zDecodeSizeKey(ek)
	case SSizeType:
		key, err = db.sDecodeSizeKey(ek)
	default:
		err = ErrDataType
	}
	return
}

// --- expire ttl ---

// val : when(expiredAt)
// | Version | CodeTypeData | uvarint DBIndex | ExpMetaType | dataType | Key |
func encodeExpMetaKey(index int, dataType byte, key []byte) []byte {
	gBuf := encodeDbIndexKey(index, CodeTypeData)
	buf := make([]byte, len(key)+2+len(gBuf))

	pos := copy(buf, gBuf)
	buf[pos] = ExpMetaType
	pos++
	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

func (db *DB) expEncodeMetaKey(dataType byte, key []byte) []byte {
	return encodeExpMetaKey(db.index, dataType, key)
}

func decodeExpMetaKey(index int, mk []byte) (byte, []byte, error) {
	pos, err := checkDbIndex(index, mk)
	if err != nil {
		return 0, nil, err
	}

	if pos+2 > len(mk) || mk[pos] != ExpMetaType {
		return 0, nil, ErrExpMetaKey
	}

	return mk[pos+1], mk[pos+2:], nil
}

func (db *DB) expDecodeMetaKey(mk []byte) (byte, []byte, error) {
	return decodeExpMetaKey(db.index, mk)
}

// for check ttl range to del expired key
// | Version | CodeTypeData | uvarint DBIndex | ExpTimeType | when | dataType | Key |
func encodeExpTimeKey(index int, dataType byte, key []byte, when int64) []byte {
	gBuf := encodeDbIndexKey(index, CodeTypeData)
	buf := make([]byte, len(key)+10+len(gBuf))

	pos := copy(buf, gBuf)

	buf[pos] = ExpTimeType
	pos++

	binary.BigEndian.PutUint64(buf[pos:], uint64(when))
	pos += 8

	buf[pos] = dataType
	pos++

	copy(buf[pos:], key)

	return buf
}

func (db *DB) expEncodeTimeKey(dataType byte, key []byte, when int64) []byte {
	return encodeExpTimeKey(db.index, dataType, key, when)
}

func decodeExpTimeKey(index int, tk []byte) (byte, []byte, int64, error) {
	pos, err := checkDbIndex(index, tk)
	if err != nil {
		return 0, nil, 0, err
	}

	if pos+10 > len(tk) || tk[pos] != ExpTimeType {
		return 0, nil, 0, ErrExpTimeKey
	}

	return tk[pos+9], tk[pos+10:], int64(binary.BigEndian.Uint64(tk[pos+1:])), nil
}

func (db *DB) expDecodeTimeKey(tk []byte) (byte, []byte, int64, error) {
	return decodeExpTimeKey(db.index, tk)
}

//--- ext binary number ---

// PutInt64 puts the 64 integer.
func PutInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

// Int64 gets 64 integer with the little endian format.
func Int64(v []byte, err error) (int64, error) {
	u, err := Uint64(v, err)
	if err != nil {
		return 0, err
	}

	return int64(u), nil
}

// Uint64 gets unsigned 64 integer with the little endian format.
func Uint64(v []byte, err error) (uint64, error) {
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, ErrIntNumber
	}

	return binary.LittleEndian.Uint64(v), nil
}
