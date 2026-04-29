// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package kmsg

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReaderBootTimeRefreshedPerMessage is a regression test for a bug where
// the boot-time offset was captured once at NewReader and added to time.Now()
// for each message. Over long-running streams that made each message's
// timestamp drift forward by the wall time elapsed since NewReader. The fix
// re-evaluates the boot time freshly for every message.
func TestReaderBootTimeRefreshedPerMessage(t *testing.T) {
	pr, pw, err := os.Pipe()
	require.NoError(t, err)

	t.Cleanup(func() {
		pw.Close() //nolint:errcheck
		pr.Close() //nolint:errcheck
	})

	var calls atomic.Int32

	// bootTimes simulates a system whose monotonic uptime advances at the
	// same rate as wall time: the boot time should remain constant across
	// calls. The buggy code (cached offset + fresh time.Now()) would instead
	// see boot time drift forward in lockstep with time.Now().
	const realBoot = "2026-04-29T10:00:00Z"

	bootTime, err := time.Parse(time.RFC3339, realBoot)
	require.NoError(t, err)

	r := &reader{
		f:       pr,
		options: options{follow: true},
		bootTime: func() time.Time {
			calls.Add(1)

			return bootTime
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := r.Scan(ctx)

	// Two messages with kernel clocks one hour apart. Both should be
	// timestamped relative to the same boot time.
	const (
		clock1 = int64(1_000_000)        // 1s after boot
		clock2 = int64(3601 * 1_000_000) // 1h 1s after boot
	)

	writeAndExpect := func(payload string, clockUs int64) {
		t.Helper()

		_, werr := pw.WriteString(payload)
		require.NoError(t, werr)

		select {
		case pkt, ok := <-ch:
			require.True(t, ok, "channel closed unexpectedly")

			require.NoError(t, pkt.Err)

			assert.Equal(t, clockUs, pkt.Message.Clock)
			assert.Equal(t, bootTime.Add(time.Duration(clockUs)*time.Microsecond), pkt.Message.Timestamp)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for packet")
		}
	}

	writeAndExpect("6,100,1000000,-;hello\n", clock1)
	writeAndExpect("6,101,3601000000,-;world\n", clock2)

	// The bootTime function must be invoked at least once per message.
	// The buggy code would call it zero times (it cached the offset).
	assert.GreaterOrEqual(t, calls.Load(), int32(2),
		"bootTime must be evaluated freshly for each message, got %d calls", calls.Load())
}

// TestReaderBootTimeRecoversAfterClockJump simulates the NTP-after-boot
// scenario the original drift-fix commit aimed at: the wall clock jumps
// forward between messages, and the per-message boot time must reflect the
// new, corrected boot time so the message timestamp lands in the corrected
// epoch rather than the pre-NTP one.
func TestReaderBootTimeRecoversAfterClockJump(t *testing.T) {
	pr, pw, err := os.Pipe()
	require.NoError(t, err)

	t.Cleanup(func() {
		pw.Close() //nolint:errcheck
		pr.Close() //nolint:errcheck
	})

	preNTP := time.Unix(5, 0).UTC()                                  // pre-NTP boot estimate (epoch + 5s)
	postNTP, err := time.Parse(time.RFC3339, "2026-04-29T10:00:00Z") // post-NTP boot estimate
	require.NoError(t, err)

	bootTimes := []time.Time{preNTP, postNTP}

	var idx atomic.Int32

	r := &reader{
		f:       pr,
		options: options{follow: true},
		bootTime: func() time.Time {
			i := int(idx.Add(1)) - 1
			if i >= len(bootTimes) {
				i = len(bootTimes) - 1
			}

			return bootTimes[i]
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := r.Scan(ctx)

	expect := func(payload string, expectedBoot time.Time, clockUs int64) {
		t.Helper()

		_, werr := pw.WriteString(payload)
		require.NoError(t, werr)

		select {
		case pkt, ok := <-ch:
			require.True(t, ok, "channel closed unexpectedly")

			require.NoError(t, pkt.Err)

			assert.Equal(t, expectedBoot.Add(time.Duration(clockUs)*time.Microsecond), pkt.Message.Timestamp)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for packet")
		}
	}

	expect("6,100,1000000,-;before-ntp\n", preNTP, 1_000_000)
	expect("6,101,2000000,-;after-ntp\n", postNTP, 2_000_000)
}
