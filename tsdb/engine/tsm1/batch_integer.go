package tsm1

import (
	"encoding/binary"
	"fmt"

	"github.com/jwilder/encoding/simple8b"
)

var (
	integerBatchDecoderFunc = [...]func(b []byte, dst []int64) ([]int64, error){
		integerBatchDecodeAllUncompressed,
		integerBatchDecodeAllSimple,
		integerBatchDecodeAllRLE,
		integerBatchDecodeAllInvalid,
	}
)

func IntegerBatchDecodeAll(b []byte, dst []int64) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}

	encoding := b[0] >> 4
	if encoding > intCompressedRLE {
		encoding = 3 // integerBatchDecodeAllInvalid
	}

	return integerBatchDecoderFunc[encoding&3](b, dst)
}

func integerBatchDecodeAllUncompressed(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) & 0x7 != 0 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: expected multiple of 8 bytes")
	}

	count := len(b) / 8
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	prev := int64(0)
	for i := range dst {
		prev += ZigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
		dst[i] = prev
	}

	return dst, nil
}

func integerBatchDecodeAllSimple(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: not enough data to decode packed value")
	}

	count, err := simple8b.CountBytes(b[8:])
	if err != nil {
		return []int64{}, err
	}

	count += 1
	if cap(dst) < count {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	j := 0

	// first value
	dst[j] = ZigZagDecode(binary.BigEndian.Uint64(b[0:8]))
	prev := dst[j]
	j++
	b = b[8:]

	var values [240]uint64
	for len(b) >= 8 {
		enc := binary.BigEndian.Uint64(b[0:8])
		n, err := simple8b.Decode(&values, enc)
		if err != nil {
			// Should never happen, only error that could be returned is if the the value to be decoded was not
			// actually encoded by simple8b encoder.
			return []int64{}, fmt.Errorf("failed to decode value %v: %v", enc, err)
		}

		for i := range values[:n] {
			prev += ZigZagDecode(values[i])
			dst[j] = prev
			j++
		}
		b = b[8:]
	}

	return dst, nil
}

func integerBatchDecodeAllRLE(b []byte, dst []int64) ([]int64, error) {
	b = b[1:]
	if len(b) < 8 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: not enough data to decode RLE starting value")
	}

	var k, n int

	// Next 8 bytes is the starting value
	first := ZigZagDecode(binary.BigEndian.Uint64(b[k : k+8]))
	k += 8

	// Next 1-10 bytes is the delta value
	value, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: invalid RLE delta value")
	}
	k += n

	delta := ZigZagDecode(value)

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[k:])
	if n <= 0 {
		return []int64{}, fmt.Errorf("IntegerBatchDecodeAll: invalid RLE repeat value")
	}

	count += 1

	if cap(dst) < int(count) {
		dst = make([]int64, count)
	} else {
		dst = dst[:count]
	}

	i := 0
	if delta == 0 {
		for i < len(dst) {
			dst[i] = first
			i++
		}
	} else {
		acc := first
		for i < len(dst) {
			dst[i] = acc
			acc += delta
			i++
		}
	}

	return dst, nil
}

func integerBatchDecodeAllInvalid(b []byte, _ []int64) ([]int64, error) {
	return []int64{}, fmt.Errorf("unknown encoding %v", b[0]>>4)
}
