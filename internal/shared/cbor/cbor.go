// SPDX-License-Identifier: Apache-2.0

package cbor

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// Common errors.
var (
	ErrUnexpectedEOF = errors.New("cbor: unexpected EOF")
	ErrUnsupported   = errors.New("cbor: unsupported type")
)

// Encode encodes a Go value (JSON-compatible) into standard CBOR bytes.
func Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeValue(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes CBOR bytes into a Go value (primitives, slices, map[string]any).
func Decode(data []byte) (any, error) {
	r := bytes.NewReader(data)
	return decodeValue(r)
}

// JSONToCBOR converts JSON bytes directly to CBOR bytes.
func JSONToCBOR(jsonData []byte) ([]byte, error) {
	if len(jsonData) == 0 {
		return []byte{}, nil
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(jsonData))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return Encode(v)
}

// CBORToJSON converts CBOR bytes directly to JSON bytes.
func CBORToJSON(cborData []byte) ([]byte, error) {
	if len(cborData) == 0 {
		return []byte("{}"), nil
	}
	v, err := Decode(cborData)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func encodeValue(w *bytes.Buffer, v any) error {
	if v == nil {
		w.WriteByte(0xf6) // null
		return nil
	}

	switch val := v.(type) {
	case bool:
		if val {
			w.WriteByte(0xf5) // true
		} else {
			w.WriteByte(0xf4) // false
		}
		return nil

	case int:
		return encodeInt(w, int64(val))
	case int8:
		return encodeInt(w, int64(val))
	case int16:
		return encodeInt(w, int64(val))
	case int32:
		return encodeInt(w, int64(val))
	case int64:
		return encodeInt(w, val)

	case uint:
		return encodeUint(w, 0, uint64(val))
	case uint8:
		return encodeUint(w, 0, uint64(val))
	case uint16:
		return encodeUint(w, 0, uint64(val))
	case uint32:
		return encodeUint(w, 0, uint64(val))
	case uint64:
		return encodeUint(w, 0, val)

	case float32:
		w.WriteByte(0xfa)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], math.Float32bits(val))
		w.Write(b[:])
		return nil

	case float64:
		w.WriteByte(0xfb)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(val))
		w.Write(b[:])
		return nil

	case json.Number:
		if i, err := val.Int64(); err == nil {
			return encodeInt(w, i)
		}
		f, err := val.Float64()
		if err != nil {
			return err
		}
		return encodeValue(w, f)

	case string:
		b := []byte(val)
		if err := encodeUint(w, 3, uint64(len(b))); err != nil {
			return err
		}
		w.Write(b)
		return nil

	case []byte:
		if err := encodeUint(w, 2, uint64(len(val))); err != nil {
			return err
		}
		w.Write(val)
		return nil

	case []any:
		if err := encodeUint(w, 4, uint64(len(val))); err != nil {
			return err
		}
		for _, item := range val {
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		return nil

	case []string:
		if err := encodeUint(w, 4, uint64(len(val))); err != nil {
			return err
		}
		for _, item := range val {
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		return nil

	case map[string]any:
		if err := encodeUint(w, 5, uint64(len(val))); err != nil {
			return err
		}
		for k, item := range val {
			if err := encodeValue(w, k); err != nil {
				return err
			}
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		return nil

	case map[string]string:
		if err := encodeUint(w, 5, uint64(len(val))); err != nil {
			return err
		}
		for k, item := range val {
			if err := encodeValue(w, k); err != nil {
				return err
			}
			if err := encodeValue(w, item); err != nil {
				return err
			}
		}
		return nil

	default:
		// Attempt JSON round-trip for structs and complex types.
		jb, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%w: %T", ErrUnsupported, v)
		}
		var generic any
		dec := json.NewDecoder(bytes.NewReader(jb))
		dec.UseNumber()
		if err := dec.Decode(&generic); err != nil {
			return err
		}
		return encodeValue(w, generic)
	}
}

func encodeInt(w *bytes.Buffer, val int64) error {
	if val >= 0 {
		return encodeUint(w, 0, uint64(val))
	}
	return encodeUint(w, 1, uint64(-1-val))
}

func encodeUint(w *bytes.Buffer, major byte, val uint64) error {
	header := major << 5
	switch {
	case val < 24:
		w.WriteByte(header | byte(val))
	case val <= 0xff:
		w.WriteByte(header | 24)
		w.WriteByte(byte(val))
	case val <= 0xffff:
		w.WriteByte(header | 25)
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], uint16(val))
		w.Write(b[:])
	case val <= 0xffffffff:
		w.WriteByte(header | 26)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(val))
		w.Write(b[:])
	default:
		w.WriteByte(header | 27)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], val)
		w.Write(b[:])
	}
	return nil
}

func decodeValue(r *bytes.Reader) (any, error) {
	b, err := r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrUnexpectedEOF
		}
		return nil, err
	}

	major := b >> 5
	info := b & 0x1f

	val, err := readInfoValue(r, info)
	if err != nil {
		return nil, err
	}

	switch major {
	case 0: // Unsigned integer
		return int64(val), nil

	case 1: // Negative integer
		return -1 - int64(val), nil

	case 2: // Byte string
		buf := make([]byte, val)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, ErrUnexpectedEOF
		}
		return buf, nil

	case 3: // Text string
		buf := make([]byte, val)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, ErrUnexpectedEOF
		}
		return string(buf), nil

	case 4: // Array
		arr := make([]any, val)
		for i := uint64(0); i < val; i++ {
			elem, err := decodeValue(r)
			if err != nil {
				return nil, err
			}
			arr[i] = elem
		}
		return arr, nil

	case 5: // Map
		m := make(map[string]any, val)
		for i := uint64(0); i < val; i++ {
			k, err := decodeValue(r)
			if err != nil {
				return nil, err
			}
			v, err := decodeValue(r)
			if err != nil {
				return nil, err
			}
			m[fmt.Sprint(k)] = v
		}
		return m, nil

	case 6: // Tag
		// Discard tag and decode underlying value (e.g. tag 1 timestamp)
		return decodeValue(r)

	case 7: // Simple / Float
		switch info {
		case 20:
			return false, nil
		case 21:
			return true, nil
		case 22, 23:
			return nil, nil
		case 25: // float16
			// 2 bytes float16
			bits := uint16(val)
			return float64(math.Float32frombits(halfToFloat32(bits))), nil
		case 26: // float32
			bits := uint32(val)
			return float64(math.Float32frombits(bits)), nil
		case 27: // float64
			bits := val
			return math.Float64frombits(bits), nil
		default:
			return nil, fmt.Errorf("%w: simple value %d", ErrUnsupported, info)
		}

	default:
		return nil, fmt.Errorf("%w: major type %d", ErrUnsupported, major)
	}
}

func readInfoValue(r *bytes.Reader, info byte) (uint64, error) {
	switch {
	case info < 24:
		return uint64(info), nil
	case info == 24:
		b, err := r.ReadByte()
		if err != nil {
			return 0, ErrUnexpectedEOF
		}
		return uint64(b), nil
	case info == 25:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, ErrUnexpectedEOF
		}
		return uint64(binary.BigEndian.Uint16(b[:])), nil
	case info == 26:
		var b [4]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, ErrUnexpectedEOF
		}
		return uint64(binary.BigEndian.Uint32(b[:])), nil
	case info == 27:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, ErrUnexpectedEOF
		}
		return binary.BigEndian.Uint64(b[:]), nil
	default:
		return 0, fmt.Errorf("%w: indefinite or reserved info %d", ErrUnsupported, info)
	}
}

func halfToFloat32(h uint16) uint32 {
	sign := (uint32(h) & 0x8000) << 16
	exp := (uint32(h) & 0x7c00) >> 10
	mant := uint32(h) & 0x03ff

	if exp == 0 {
		if mant == 0 {
			return sign
		}
		for (mant & 0x0400) == 0 {
			mant <<= 1
			exp--
		}
		exp++
		mant &= ^uint32(0x0400)
		return sign | ((exp + 112) << 23) | (mant << 13)
	}
	if exp == 31 {
		return sign | 0x7f800000 | (mant << 13)
	}
	return sign | ((exp + 112) << 23) | (mant << 13)
}
