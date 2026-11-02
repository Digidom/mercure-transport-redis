// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// addForeignEntry writes an update to the stream the way another instance would,
// without any Pub/Sub message.
func addForeignEntry(t *testing.T, client *redis.Client, id, data string) string {
	t.Helper()

	payload, err := json.Marshal(mercure.Update{
		Event:  mercure.Event{ID: id, Data: data},
		Topics: []string{"/test/topic"},
	})
	require.NoError(t, err)

	streamID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "mercure",
		Values: map[string]interface{}{"data": string(payload)},
	}).Result()
	require.NoError(t, err)

	return streamID
}

func currentLastEventIDOf(transport *RedisTransport) string {
	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	return transport.lastEventID
}

func TestCatchUp_DeliversEntriesMissedByPubSub(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	received := sseCollector(t, transport)

	streamID := addForeignEntry(t, client, "urn:uuid:missed", "missed")

	transport.catchUp(context.Background())

	require.Eventually(t, func() bool { return contains(received(), "missed") },
		2*time.Second, 10*time.Millisecond)
	assert.Equal(t, "urn:uuid:missed", currentLastEventIDOf(transport))
	assert.Equal(t, streamID, transport.streamCursor)
}

func TestCatchUp_DoesNotRedeliverLocalOrPubSubUpdates(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	received := sseCollector(t, transport)
	ctx := context.Background()

	require.NoError(t, transport.Dispatch(ctx, newUpdate("local")))

	// Simulates an update first delivered live by Pub/Sub, then found in the stream.
	pubsubUpdate := newUpdate("via-pubsub")
	require.NoError(t, transport.Dispatch(ctx, pubsubUpdate))

	transport.catchUp(ctx)
	transport.catchUp(ctx)

	require.NoError(t, transport.Dispatch(ctx, newUpdate("marker")))
	require.Eventually(t, func() bool { return contains(received(), "marker") },
		2*time.Second, 10*time.Millisecond)

	assert.Equal(t, 1, count(received(), "local"))
	assert.Equal(t, 1, count(received(), "via-pubsub"))
}

func TestCatchUp_StartsAfterCursor(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)

	addForeignEntry(t, client, "urn:uuid:old", "old")
	transport.streamCursor = transport.fetchLatestStreamID(context.Background())
	addForeignEntry(t, client, "urn:uuid:new", "new")

	transport.catchUp(context.Background())

	assert.True(t, transport.delivered.add("urn:uuid:old"), "entries at or before the cursor must not be delivered")
	assert.False(t, transport.delivered.add("urn:uuid:new"))
}

func TestCatchUp_SkipsInvalidEntriesAndAdvancesCursor(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{Stream: "mercure", Values: map[string]interface{}{"data": "{not json"}}).Err())
	require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{Stream: "mercure", Values: map[string]interface{}{"other": "x"}}).Err())
	lastID := addForeignEntry(t, client, "urn:uuid:valid", "valid")

	transport.catchUp(ctx)

	assert.Equal(t, "urn:uuid:valid", currentLastEventIDOf(transport))
	assert.Equal(t, lastID, transport.streamCursor)
}

func TestCatchUp_PagesThroughLargeBacklog(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	transport.size = 0

	total := int(historyPageSize) + 500
	for i := range total {
		addForeignEntry(t, client, fmt.Sprintf("urn:uuid:backlog-%d", i), "x")
	}

	transport.catchUp(context.Background())

	assert.Equal(t, fmt.Sprintf("urn:uuid:backlog-%d", total-1), currentLastEventIDOf(transport))
}

func TestCatchUp_ResumesFromOldestEntryWhenCursorWasTrimmed(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)

	transport.streamCursor = "1-0" // older than anything left in the stream
	addForeignEntry(t, client, "urn:uuid:survivor", "survivor")

	transport.catchUp(context.Background())

	assert.Equal(t, "urn:uuid:survivor", currentLastEventIDOf(transport))
}

func TestCatchUp_KeepsCursorWhenRedisIsDown(t *testing.T) {
	t.Parallel()

	transport, _, server := newTestTransport(t)
	transport.streamCursor = "42-0"
	server.Close()

	transport.catchUp(context.Background())

	assert.Equal(t, "42-0", transport.streamCursor)
}

func TestDispatch_ReturnsNilAndDeliversLocallyWhenRedisIsDown(t *testing.T) {
	t.Parallel()

	transport, _, server := newTestTransport(t)
	received := sseCollector(t, transport)
	server.Close()

	require.NoError(t, transport.Dispatch(context.Background(), newUpdate("offline")))

	require.Eventually(t, func() bool { return contains(received(), "offline") },
		2*time.Second, 10*time.Millisecond)
}

func TestDeliveredSet(t *testing.T) {
	t.Parallel()

	set := newDeliveredSet(2)

	assert.True(t, set.add("a"))
	assert.False(t, set.add("a"), "duplicates are rejected")
	assert.True(t, set.add("b"))
	assert.True(t, set.add("c"), "evicts a")
	assert.False(t, set.add("b"))
	assert.False(t, set.add("c"))
	assert.True(t, set.add("a"), "a was evicted")
	assert.True(t, set.add(""), "an empty ID cannot be deduplicated")
	assert.True(t, set.add(""))
}
