// SPDX-License-Identifier: Apache-2.0

package cbor

import (
	"reflect"
	"testing"
)

func TestCBOREncodeDecodePrimitives(t *testing.T) {
	cases := []struct {
		name  string
		input any
	}{
		{"nil", nil},
		{"true", true},
		{"false", false},
		{"zero", int64(0)},
		{"small_pos", int64(23)},
		{"med_pos", int64(255)},
		{"large_pos", int64(65535)},
		{"huge_pos", int64(1000000000)},
		{"small_neg", int64(-1)},
		{"med_neg", int64(-500)},
		{"float", 3.14159},
		{"empty_str", ""},
		{"hello_str", "hello, world"},
		{"bytes", []byte{1, 2, 3, 4, 5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Encode(tc.input)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}
			if tc.input == nil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			// For numbers, compare value
			switch v := tc.input.(type) {
			case int:
				if got != int64(v) {
					t.Fatalf("expected %v, got %v", v, got)
				}
			case float64:
				if diff := got.(float64) - v; diff > 0.0001 || diff < -0.0001 {
					t.Fatalf("expected %v, got %v", v, got)
				}
			default:
				if !reflect.DeepEqual(got, tc.input) {
					t.Fatalf("expected %v, got %v", tc.input, got)
				}
			}
		})
	}
}

func TestCBOREncodeDecodeComposite(t *testing.T) {
	inputMap := map[string]any{
		"name":  "devcloud",
		"count": int64(42),
		"tags":  []any{"a", "b", "c"},
		"nested": map[string]any{
			"active": true,
			"score":  99.5,
		},
	}

	b, err := Encode(inputMap)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", decoded)
	}

	if m["name"] != "devcloud" {
		t.Errorf("expected name=devcloud, got %v", m["name"])
	}
	if m["count"] != int64(42) {
		t.Errorf("expected count=42, got %v", m["count"])
	}
}

func TestJSONRoundTrip(t *testing.T) {
	jsonInput := []byte(`{"message":"Resource not found","code":"ResourceNotFoundException","count":10}`)
	cborBytes, err := JSONToCBOR(jsonInput)
	if err != nil {
		t.Fatalf("JSONToCBOR failed: %v", err)
	}

	jsonOutput, err := CBORToJSON(cborBytes)
	if err != nil {
		t.Fatalf("CBORToJSON failed: %v", err)
	}

	decoded, err := Decode(cborBytes)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	m := decoded.(map[string]any)
	if m["code"] != "ResourceNotFoundException" {
		t.Errorf("unexpected code: %v", m["code"])
	}
	if len(jsonOutput) == 0 {
		t.Errorf("empty json output")
	}
}
