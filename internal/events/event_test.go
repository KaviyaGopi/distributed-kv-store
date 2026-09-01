package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KaviyaGopi/distributed-kv-store/internal/fsm"
)

func TestKeyChangedEncodeRoundTrip(t *testing.T) {
	evt := KeyChanged{
		Op:        fsm.OpPut,
		Key:       "foo",
		Value:     "bar",
		Shard:     1,
		RaftIndex: 42,
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := evt.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var decoded KeyChanged
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded != evt {
		t.Fatalf("round trip mismatch: got %+v, want %+v", decoded, evt)
	}
}
