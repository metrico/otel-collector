package chstatsreceiver

import (
	"testing"
	"time"
)

func TestNextTick(t *testing.T) {
	const interval = 15 * time.Second
	at := func(s float64) time.Time { return time.Unix(0, int64(s*1e9)) }
	for _, tc := range []struct{ prev, now, want float64 }{
		{0, 0, 7.5},
		{0, 10, 22.5},
		{7.5, 7.5, 22.5},
		{7.5, 7.4, 22.5},
		{7.5, 40, 52.5},
		{3607.5, 10, 22.5},
	} {
		if got := nextTick(at(tc.prev), at(tc.now), interval); !got.Equal(at(tc.want)) {
			t.Errorf("prev %v now %v: got %v, want %v", tc.prev, tc.now, got.Sub(at(0)).Seconds(), tc.want)
		}
	}
}
