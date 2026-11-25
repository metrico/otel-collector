package chstatsreceiver

import (
	"testing"
	"time"
)

func TestNextRunIsInTheMiddleOfTheInterval(t *testing.T) {
	at := func(s string) time.Time {
		v, err := time.Parse("15:04:05.000", s)
		if err != nil {
			t.Fatal(err)
		}
		return time.Date(2026, 11, 25, v.Hour(), v.Minute(), v.Second(), v.Nanosecond(), time.UTC)
	}
	for _, tc := range []struct {
		now, want string
		interval  time.Duration
	}{
		{"17:00:00.000", "17:00:07.500", 15 * time.Second},
		{"17:00:07.499", "17:00:07.500", 15 * time.Second},
		{"17:00:07.500", "17:00:22.500", 15 * time.Second},
		{"17:00:14.999", "17:00:22.500", 15 * time.Second},
		{"23:59:59.000", "00:00:07.500", 15 * time.Second},
		{"17:00:40.000", "17:01:30.000", time.Minute},
	} {
		want := at(tc.want)
		if tc.want == "00:00:07.500" {
			want = want.AddDate(0, 0, 1)
		}
		if got := nextRun(at(tc.now), tc.interval); !got.Equal(want) {
			t.Errorf("after %s got %s, want %s", tc.now, got.Format("15:04:05.000"), tc.want)
		}
	}
}

// A long or early run must not skip an interval that is still ahead, nor repeat
// one, whatever moment the receiver starts at.
func TestEveryIntervalGetsOneRun(t *testing.T) {
	const interval = 15 * time.Second
	for startMs := 0; startMs < 15000; startMs += 250 {
		start := time.Date(2026, 11, 25, 17, 0, 0, startMs*1e6, time.UTC)
		next := nextRun(start, interval)
		slots := map[int64]int{}
		for i := range 400 {
			slots[next.Unix()/15]++
			// The run ends from 2 ms early (clock adjusted) to 298 ms late.
			now := next.Add(time.Duration(i%7*50-2) * time.Millisecond)
			if now.Before(next) {
				now = next
			}
			next = nextRun(now, interval)
		}
		if len(slots) != 400 {
			t.Fatalf("start +%d ms: %d intervals with a point, want 400", startMs, len(slots))
		}
	}
}
