// Package blobqueue is a durable, restart-surviving, acknowledged outbound queue for OPAQUE BINARY
// bodies (OTLP/HTTP payloads, possibly gzip). It is a sibling of eventoutbox, not an extension: the
// wire frame here is length-prefixed binary, so a body is stored and sent byte-for-byte with no base64.
//
// stdlib only. The caller supplies the *sql.DB (open it WAL with a busy_timeout, and synchronous=FULL
// if power loss must not lose acknowledged rows; see Durability). Design: docs/BLOBQUEUE_DESIGN.md.
package blobqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// SignalLoss is the reserved signal of the loss-marker item (its body is a JSON LossMarker).
const SignalLoss = "loss"

const (
	frameMagic0  = 'B'
	frameMagic1  = 'Q'
	FrameVersion = 1
)

// Item is one queued body. Seq is assigned by the queue; Encode/Decode carry it unchanged.
type Item struct {
	Seq             uint64
	Signal          string // logs | metrics | traces | SignalLoss
	ContentType     string
	ContentEncoding string
	Body            []byte
}

// Limits bound what DecodeFrame will allocate from untrusted input.
type Limits struct {
	MaxItems     int // default 4096
	MaxBodyBytes int // default 4 MiB
}

func (l Limits) norm() Limits {
	if l.MaxItems <= 0 {
		l.MaxItems = 4096
	}
	if l.MaxBodyBytes <= 0 {
		l.MaxBodyBytes = 4 << 20
	}
	return l
}

// ErrBadFrame wraps every decode refusal.
var ErrBadFrame = errors.New("blobqueue: bad frame")

// EncodedSize is the exact length EncodeFrame will produce for the given epoch and items.
func EncodedSize(epoch string, items []Item) int {
	n := 2 + 1 + 1 + len(epoch) + 4
	for i := range items {
		n += itemSize(&items[i])
	}
	return n
}

func itemSize(it *Item) int {
	return 8 + 1 + len(it.Signal) + 2 + len(it.ContentType) + 2 + len(it.ContentEncoding) + 4 + len(it.Body)
}

// EncodeFrame appends the frame for items to dst. epoch must be <=255 bytes, Signal <=255,
// ContentType/ContentEncoding <=65535 (the queue enforces this at Enqueue).
func EncodeFrame(dst []byte, epoch string, items []Item) ([]byte, error) {
	if len(epoch) > 255 {
		return dst, fmt.Errorf("%w: epoch too long", ErrBadFrame)
	}
	dst = append(dst, frameMagic0, frameMagic1, FrameVersion, byte(len(epoch)))
	dst = append(dst, epoch...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(items)))
	for i := range items {
		it := &items[i]
		if len(it.Signal) > 255 || len(it.ContentType) > 0xFFFF || len(it.ContentEncoding) > 0xFFFF {
			return dst, fmt.Errorf("%w: field too long", ErrBadFrame)
		}
		dst = binary.LittleEndian.AppendUint64(dst, it.Seq)
		dst = append(dst, byte(len(it.Signal)))
		dst = append(dst, it.Signal...)
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(it.ContentType)))
		dst = append(dst, it.ContentType...)
		dst = binary.LittleEndian.AppendUint16(dst, uint16(len(it.ContentEncoding)))
		dst = append(dst, it.ContentEncoding...)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(it.Body)))
		dst = append(dst, it.Body...)
	}
	return dst, nil
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.err = fmt.Errorf("%w: truncated", ErrBadFrame)
		return nil
	}
	out := r.b[:n:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() int {
	if b := r.take(1); b != nil {
		return int(b[0])
	}
	return 0
}

func (r *reader) u16() int {
	if b := r.take(2); b != nil {
		return int(binary.LittleEndian.Uint16(b))
	}
	return 0
}

func (r *reader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}

func (r *reader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}

// DecodeFrame parses a frame. Every length is checked against the remaining buffer and the limits
// BEFORE allocating, and trailing bytes are refused. Bodies alias frame (no copy): copy before
// retaining past the frame's lifetime.
func DecodeFrame(frame []byte, lim Limits) (epoch string, items []Item, err error) {
	lim = lim.norm()
	r := &reader{b: frame}
	head := r.take(3)
	if r.err != nil || head[0] != frameMagic0 || head[1] != frameMagic1 {
		return "", nil, fmt.Errorf("%w: magic", ErrBadFrame)
	}
	if head[2] != FrameVersion {
		return "", nil, fmt.Errorf("%w: unsupported version %d", ErrBadFrame, head[2])
	}
	epoch = string(r.take(r.u8()))
	count := r.u32()
	if r.err != nil {
		return "", nil, r.err
	}
	if int64(count) > int64(lim.MaxItems) {
		return "", nil, fmt.Errorf("%w: %d items over limit %d", ErrBadFrame, count, lim.MaxItems)
	}
	// An item is at least 17 bytes, so the buffer bounds count before the slice is allocated.
	if int64(count)*17 > int64(len(r.b)) {
		return "", nil, fmt.Errorf("%w: truncated", ErrBadFrame)
	}
	items = make([]Item, 0, count)
	for i := uint32(0); i < count; i++ {
		var it Item
		it.Seq = r.u64()
		it.Signal = string(r.take(r.u8()))
		it.ContentType = string(r.take(r.u16()))
		it.ContentEncoding = string(r.take(r.u16()))
		bl := r.u32()
		if int64(bl) > int64(lim.MaxBodyBytes) {
			return "", nil, fmt.Errorf("%w: body %d over limit %d", ErrBadFrame, bl, lim.MaxBodyBytes)
		}
		it.Body = r.take(int(bl))
		if r.err != nil {
			return "", nil, r.err
		}
		items = append(items, it)
	}
	if len(r.b) != 0 {
		return "", nil, fmt.Errorf("%w: %d trailing bytes", ErrBadFrame, len(r.b))
	}
	return epoch, items, nil
}
