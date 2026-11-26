package blobqueue

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func sampleItems() []Item {
	return []Item{
		{Seq: 1, Signal: "logs", ContentType: "application/x-protobuf", ContentEncoding: "gzip", Body: []byte{0, 1, 2, 0xff, 0}},
		{Seq: 7, Signal: "metrics", ContentType: "application/x-protobuf", Body: nil},
		{Seq: 1 << 40, Signal: SignalLoss, Body: []byte(`{"rows":3}`)},
	}
}

func TestFrameRoundTrip(t *testing.T) {
	items := sampleItems()
	frame, err := EncodeFrame(nil, "ep1", items)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(frame), EncodedSize("ep1", items); got != want {
		t.Fatalf("EncodedSize %d != encoded %d", want, got)
	}
	ep, out, err := DecodeFrame(frame, Limits{})
	if err != nil || ep != "ep1" {
		t.Fatalf("decode: %v ep=%q", err, ep)
	}
	if len(out) != len(items) {
		t.Fatalf("items %d != %d", len(out), len(items))
	}
	for i := range items {
		a, b := items[i], out[i]
		if a.Seq != b.Seq || a.Signal != b.Signal || a.ContentType != b.ContentType ||
			a.ContentEncoding != b.ContentEncoding || !bytes.Equal(a.Body, b.Body) {
			t.Fatalf("item %d: %+v != %+v", i, a, b)
		}
	}
}

func TestFrameEmpty(t *testing.T) {
	frame, _ := EncodeFrame(nil, "", nil)
	ep, out, err := DecodeFrame(frame, Limits{})
	if err != nil || ep != "" || len(out) != 0 {
		t.Fatalf("empty: %v %q %d", err, ep, len(out))
	}
}

func TestDecodeRefusals(t *testing.T) {
	good, _ := EncodeFrame(nil, "e", sampleItems())
	cases := []struct {
		name string
		in   []byte
		lim  Limits
	}{
		{"nil", nil, Limits{}},
		{"bad magic", append([]byte("XQ"), good[2:]...), Limits{}},
		{"bad version", append(append([]byte{}, good[:2]...), append([]byte{9}, good[3:]...)...), Limits{}},
		{"truncated head", good[:5], Limits{}},
		{"truncated body", good[:len(good)-1], Limits{}},
		{"trailing", append(append([]byte{}, good...), 0), Limits{}},
		{"item limit", good, Limits{MaxItems: 2}},
		{"body limit", good, Limits{MaxBodyBytes: 4}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := DecodeFrame(c.in, c.lim)
			if !errors.Is(err, ErrBadFrame) {
				t.Fatalf("want ErrBadFrame, got %v", err)
			}
		})
	}
}

// A hostile count must be refused before any allocation sized by it.
func TestDecodeHugeCountNoAlloc(t *testing.T) {
	frame := []byte{'B', 'Q', FrameVersion, 0, 0xff, 0xff, 0xff, 0x7f}
	allocs := testing.AllocsPerRun(10, func() {
		_, _, _ = DecodeFrame(frame, Limits{MaxItems: 1 << 30})
	})
	// Only the error value and epoch string allocate; a count-sized slice would be a 2 GiB make
	// that crashes the test process, so reaching here at all proves it was refused first.
	if allocs > 8 {
		t.Fatalf("hostile count allocated %v times", allocs)
	}
}

func TestEncodeFieldLimits(t *testing.T) {
	if _, err := EncodeFrame(nil, strings.Repeat("e", 256), nil); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("long epoch: %v", err)
	}
	if _, err := EncodeFrame(nil, "e", []Item{{Signal: strings.Repeat("s", 256)}}); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("long signal: %v", err)
	}
}

func FuzzDecodeFrame(f *testing.F) {
	good, _ := EncodeFrame(nil, "ep", sampleItems())
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{'B', 'Q', 1, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, in []byte) {
		ep, items, err := DecodeFrame(in, Limits{MaxItems: 64, MaxBodyBytes: 1 << 16})
		if err != nil {
			return
		}
		// Anything that decodes must re-encode to the identical bytes (canonical form).
		again, err := EncodeFrame(nil, ep, items)
		if err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		if !reflect.DeepEqual(again, in) {
			t.Fatalf("not canonical: %x vs %x", again, in)
		}
	})
}
