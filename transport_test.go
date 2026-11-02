// Copyright (C) 2026 desk.ly GmbH
// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The listenPubSub goroutine is NOT started to keep tests deterministic.
func newTestTransport(t *testing.T) (*RedisTransport, *redis.Client, *miniredis.Miniredis) {
	t.Helper()

	miniredisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniredisServer.Addr()})
	_, cancel := context.WithCancel(context.Background())

	transport := &RedisTransport{
		subscribers:  mercure.NewSubscriberList(1000),
		logger:       slog.Default(),
		client:       client,
		stream:       "mercure",
		channel:      "mercure:pubsub",
		indexKey:     "mercure:index",
		instanceID:   generateInstanceID(),
		size:         1000,
		historyLimit: 1000,
		closed:       make(chan struct{}),
		cancel:       cancel,
		lastEventID:  mercure.EarliestLastEventID,
		delivered:    newDeliveredSet(deliveredSetSize),
		streamCursor: "0-0",
	}

	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport, client, miniredisServer
}

func newUpdate(data string) *mercure.Update {
	return &mercure.Update{
		Event:  mercure.Event{Data: data},
		Topics: []string{"/test/topic"},
	}
}

func seedStream(t *testing.T, transport *RedisTransport, count int) []*mercure.Update {
	t.Helper()

	ctx := context.Background()
	updates := make([]*mercure.Update, count)

	for i := range count {
		updates[i] = newUpdate(fmt.Sprintf("seed-%d", i))
		require.NoError(t, transport.Dispatch(ctx, updates[i]))
	}

	return updates
}

func TestGenerateInstanceID(t *testing.T) {
	t.Parallel()

	first := generateInstanceID()
	second := generateInstanceID()

	assert.Len(t, first, 32, "instance ID should be 32 hex chars (16 bytes)")
	assert.Len(t, second, 32)
	assert.NotEqual(t, first, second, "two generated IDs must be unique")
}

func TestNewRedisTransport(t *testing.T) {
	t.Parallel()

	miniredisServer := miniredis.RunT(t)

	transport, err := NewRedisTransport(
		mercure.NewSubscriberList(1000),
		slog.Default(),
		"redis://"+miniredisServer.Addr(),
		"custom-stream",
		500,
		2000,
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	assert.Equal(t, "custom-stream", transport.stream)
	assert.Equal(t, "custom-stream:pubsub", transport.channel)
	assert.Equal(t, int64(500), transport.size)
	assert.Equal(t, int64(2000), transport.historyLimit)
	assert.NotEmpty(t, transport.instanceID)
}

func TestNewRedisTransport_DefaultStream(t *testing.T) {
	t.Parallel()

	miniredisServer := miniredis.RunT(t)

	transport, err := NewRedisTransport(
		mercure.NewSubscriberList(1000),
		slog.Default(),
		"redis://"+miniredisServer.Addr(),
		"",
		100,
		1000,
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	assert.Equal(t, "mercure", transport.stream)
	assert.Equal(t, "mercure:pubsub", transport.channel)
}

func TestNewRedisTransport_InvalidURL(t *testing.T) {
	t.Parallel()

	_, err := NewRedisTransport(
		mercure.NewSubscriberList(1000),
		slog.Default(),
		"not-a-valid-url",
		"",
		100,
		1000,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Redis URL")
}

func TestNewRedisTransport_ConnectionFailure(t *testing.T) {
	t.Parallel()

	_, err := NewRedisTransport(
		mercure.NewSubscriberList(1000),
		slog.Default(),
		"redis://127.0.0.1:1",
		"",
		100,
		1000,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to connect to Redis")
}

func TestNewRedisTransport_FetchesLastEventIDFromStream(t *testing.T) {
	t.Parallel()

	miniredisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: miniredisServer.Addr()})
	ctx := context.Background()

	update := mercure.Update{Event: mercure.Event{ID: "urn:uuid:pre-existing"}, Topics: []string{"/test/topic"}}
	data, err := json.Marshal(update)
	require.NoError(t, err)

	err = client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mercure",
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
	require.NoError(t, err)

	transport, err := NewRedisTransport(
		mercure.NewSubscriberList(1000),
		slog.Default(),
		"redis://"+miniredisServer.Addr(),
		"mercure",
		100,
		1000,
	)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(ctx) })

	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	assert.Equal(t, "urn:uuid:pre-existing", transport.lastEventID)
}

func TestDispatch(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("hello")
	update.Topics = []string{"/books/1"}

	require.NoError(t, transport.Dispatch(ctx, update))
	assert.NotEmpty(t, update.ID, "AssignUUID should have set an ID")

	messages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, messages, 1)

	rawData, ok := messages[0].Values["data"].(string)
	require.True(t, ok)

	var stored mercure.Update
	require.NoError(t, json.Unmarshal([]byte(rawData), &stored))
	assert.Equal(t, update.ID, stored.ID)
	assert.Equal(t, []string{"/books/1"}, stored.Topics)
	assert.Equal(t, "hello", stored.Data)

	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	assert.Equal(t, update.ID, transport.lastEventID)
}

func TestDispatch_MultipleMessages(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()
	messageCount := 5

	ids := make([]string, messageCount)
	for i := range messageCount {
		update := newUpdate(fmt.Sprintf("msg-%d", i))
		require.NoError(t, transport.Dispatch(ctx, update))
		ids[i] = update.ID
	}

	messages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, messages, messageCount)

	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	assert.Equal(t, ids[messageCount-1], transport.lastEventID)
}

func TestDispatch_ClosedTransport(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	transport.Close(context.Background())

	err := transport.Dispatch(context.Background(), newUpdate("should-fail"))
	assert.ErrorIs(t, err, mercure.ErrClosedTransport)
}

func TestDispatch_PublishesPubSubMessage(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	subscription := client.Subscribe(ctx, transport.channel)
	defer subscription.Close()

	_, err := subscription.Receive(ctx)
	require.NoError(t, err)

	update := newUpdate("pub-test")
	require.NoError(t, transport.Dispatch(ctx, update))

	received, err := subscription.ReceiveMessage(ctx)
	require.NoError(t, err)

	var receivedMessage pubsubMessage
	require.NoError(t, json.Unmarshal([]byte(received.Payload), &receivedMessage))
	assert.Equal(t, transport.instanceID, receivedMessage.InstanceID)
	assert.Equal(t, update.ID, receivedMessage.Update.ID)
	assert.Equal(t, "pub-test", receivedMessage.Update.Data)
}

func TestTrim_NoTrimWhenSizeZero(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	transport.size = 0
	ctx := context.Background()

	messageCount := 50
	for range messageCount {
		require.NoError(t, transport.Dispatch(ctx, newUpdate("no-trim")))
	}

	transport.trim(ctx)

	messages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, messages, messageCount, "no trimming should occur when size is 0")
}

func TestDispatch_PreservesStreamOrder(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()
	messageCount := 20

	expectedData := make([]string, messageCount)
	for i := range messageCount {
		expectedData[i] = fmt.Sprintf("ordered-%d", i)
		require.NoError(t, transport.Dispatch(ctx, newUpdate(expectedData[i])))
	}

	messages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, messages, messageCount)

	for i, message := range messages {
		rawData, ok := message.Values["data"].(string)
		require.True(t, ok)

		var stored mercure.Update
		require.NoError(t, json.Unmarshal([]byte(rawData), &stored))
		assert.Equal(t, expectedData[i], stored.Data, "message at index %d should preserve insertion order", i)
	}
}

func TestDispatch_EachUpdateGetsUniqueID(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()
	messageCount := 50

	seen := make(map[string]struct{}, messageCount)
	for range messageCount {
		update := newUpdate("unique-check")
		require.NoError(t, transport.Dispatch(ctx, update))
		_, duplicate := seen[update.ID]
		assert.False(t, duplicate, "duplicate UUID detected: %s", update.ID)
		seen[update.ID] = struct{}{}
	}

	assert.Len(t, seen, messageCount)
}

func TestTrim_BoundsStreamToConfiguredSize(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	transport.size = 50
	ctx := context.Background()

	for range 200 {
		require.NoError(t, transport.Dispatch(ctx, newUpdate("trim-verify")))
	}

	// Dispatch no longer trims. The stream holds every write until trim runs.
	streamLength, err := client.XLen(ctx, "mercure").Result()
	require.NoError(t, err)
	require.Equal(t, int64(200), streamLength)

	transport.trim(ctx)

	streamLength, err = client.XLen(ctx, "mercure").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(50), streamLength, "exact MAXLEN must cap the stream at the configured size")
}

func TestDispatch_WritesToConfiguredStream(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	transport.stream = "custom-events"
	transport.channel = "custom-events:pubsub"
	ctx := context.Background()

	require.NoError(t, transport.Dispatch(ctx, newUpdate("custom-stream-test")))

	customMessages, err := client.XRange(ctx, "custom-events", "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, customMessages, 1)

	defaultMessages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	assert.Empty(t, defaultMessages, "default stream must remain empty when custom stream is configured")
}

func TestDispatch_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()
	goroutineCount := 50

	var waitGroup sync.WaitGroup
	waitGroup.Add(goroutineCount)

	for i := range goroutineCount {
		go func() {
			defer waitGroup.Done()
			assert.NoError(t, transport.Dispatch(ctx, newUpdate(fmt.Sprintf("concurrent-%d", i))))
		}()
	}

	waitGroup.Wait()

	messages, err := client.XRange(ctx, "mercure", "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, messages, goroutineCount, "all concurrent dispatches must be persisted")
}

func TestDispatch_ConcurrentWithClose(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for range 100 {
			if err := transport.Dispatch(ctx, newUpdate("race-with-close")); err != nil {
				return
			}
		}
	}()

	go func() {
		defer waitGroup.Done()
		assert.NoError(t, transport.Close(ctx))
	}()

	waitGroup.Wait()
}

func TestFetchLastEventID_EmptyStream(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	assert.Equal(t, mercure.EarliestLastEventID, transport.fetchLastEventID())
}

func TestFetchLastEventID_WithEvents(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("single-event")
	require.NoError(t, transport.Dispatch(ctx, update))

	assert.Equal(t, update.ID, transport.fetchLastEventID())
}

func TestFetchLastEventID_ReturnsLatest(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	updates := seedStream(t, transport, 3)

	assert.Equal(t, updates[2].ID, transport.fetchLastEventID())
}

func TestFetchLastEventID_InvalidJSON(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mercure",
		Values: map[string]interface{}{"data": "not-json"},
	}).Err()
	require.NoError(t, err)

	assert.Equal(t, mercure.EarliestLastEventID, transport.fetchLastEventID())
}

func TestFetchLastEventID_MissingDataField(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mercure",
		Values: map[string]interface{}{"unexpected-key": "some-value"},
	}).Err()
	require.NoError(t, err)

	assert.Equal(t, mercure.EarliestLastEventID, transport.fetchLastEventID())
}

func TestFetchLastEventID_EmptyUpdateID(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	update := mercure.Update{Topics: []string{"/test/topic"}}
	data, err := json.Marshal(update)
	require.NoError(t, err)

	err = client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mercure",
		Values: map[string]interface{}{"data": string(data)},
	}).Err()
	require.NoError(t, err)

	// Returns the empty ID from the Update, which the transport does not treat as an error.
	assert.Equal(t, "", transport.fetchLastEventID())
}

func newTestHubServer(t *testing.T, transport *RedisTransport) *httptest.Server {
	t.Helper()

	hub, err := mercure.NewHub(context.Background(), mercure.WithAnonymous(), mercure.WithTransport(transport))
	require.NoError(t, err)

	server := httptest.NewServer(hub)
	t.Cleanup(func() {
		// Close would wait forever on a handler stuck without HistoryDispatched.
		if !t.Failed() {
			server.Close()
		}
	})

	return server
}

func subscribeWithLastEventID(t *testing.T, server *httptest.Server, lastEventID string) *http.Response {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/.well-known/mercure?match=/test/topic", nil)
	require.NoError(t, err)
	request.Header.Set("Last-Event-ID", lastEventID)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "the hub must send headers instead of waiting for the history dispatch")
	t.Cleanup(func() { response.Body.Close() })

	return response
}

func TestAddSubscriber_EmptyLastEventIDDoesNotBlockSubscribe(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	seedStream(t, transport, 1)

	response := subscribeWithLastEventID(t, newTestHubServer(t, transport), "")

	assert.Equal(t, http.StatusOK, response.StatusCode)
	assert.Equal(t, mercure.EarliestLastEventID, response.Header.Get("Mercure-Last-Event-ID"))
}

func TestAddSubscriber_ReplaysHistoryAfterLastEventID(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	updates := seedStream(t, transport, 2)

	response := subscribeWithLastEventID(t, newTestHubServer(t, transport), updates[0].ID)

	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		require.NotEqual(t, "id: "+updates[0].ID, scanner.Text(), "the anchor itself must not be replayed")

		if scanner.Text() == "id: "+updates[1].ID {
			return
		}
	}

	t.Fatalf("event after the anchor was not replayed: %v", scanner.Err())
}

func TestGetSubscribers_EmptyTransport(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	lastEventID, subscribers, err := transport.GetSubscribers(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mercure.EarliestLastEventID, lastEventID)
	assert.Empty(t, subscribers)
}

func TestGetSubscribers_ReturnsLastEventID(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("subscriber-check")
	require.NoError(t, transport.Dispatch(ctx, update))

	lastEventID, _, err := transport.GetSubscribers(ctx)
	require.NoError(t, err)
	assert.Equal(t, update.ID, lastEventID)
}

func TestGetSubscribers_AfterClose(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("before-close")
	require.NoError(t, transport.Dispatch(ctx, update))

	require.NoError(t, transport.Close(ctx))

	lastEventID, subscribers, err := transport.GetSubscribers(ctx)
	require.NoError(t, err)
	assert.Equal(t, update.ID, lastEventID, "lastEventID must survive close")
	assert.Empty(t, subscribers, "all subscribers must be disconnected after close")
}

func TestGetSubscribers_ConcurrentWithDispatch(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for range 100 {
			transport.Dispatch(ctx, newUpdate("concurrent-read")) //nolint:errcheck
		}
	}()

	go func() {
		defer waitGroup.Done()
		for range 100 {
			_, _, err := transport.GetSubscribers(ctx)
			assert.NoError(t, err)
		}
	}()

	waitGroup.Wait()
}

func TestClose(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	require.NoError(t, transport.Close(context.Background()))

	select {
	case <-transport.closed:
	default:
		t.Fatal("closed channel should be closed")
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	require.NoError(t, transport.Close(context.Background()))
	require.NoError(t, transport.Close(context.Background()), "double close must not panic or error")
}

func TestSubscribePubSub_SkipsOwnInstanceMessages(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx, cancel := context.WithCancel(context.Background())

	ownMessage := pubsubMessage{
		InstanceID: transport.instanceID,
		Update:     &mercure.Update{Event: mercure.Event{ID: "urn:uuid:self"}, Topics: []string{"/test/topic"}},
	}
	payload, err := json.Marshal(ownMessage)
	require.NoError(t, err)

	done := make(chan bool)
	go func() {
		done <- transport.subscribePubSub(ctx)
	}()

	require.Eventually(t, func() bool {
		return client.PubSubNumSub(ctx, transport.channel).Val()[transport.channel] > 0
	}, 2*time.Second, 5*time.Millisecond, "subscriber must connect before publish")

	err = client.Publish(ctx, transport.channel, string(payload)).Err()
	require.NoError(t, err)

	cancel()
	<-done

	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	assert.Equal(t, mercure.EarliestLastEventID, transport.lastEventID,
		"self-dispatched pub/sub messages must not update lastEventID")
}

func TestSubscribePubSub_UpdatesLastEventIDFromOtherInstance(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx, cancel := context.WithCancel(context.Background())

	foreignMessage := pubsubMessage{
		InstanceID: "other-instance-id",
		Update:     &mercure.Update{Event: mercure.Event{ID: "urn:uuid:foreign"}, Topics: []string{"/test/topic"}},
	}
	payload, err := json.Marshal(foreignMessage)
	require.NoError(t, err)

	done := make(chan bool)
	go func() {
		done <- transport.subscribePubSub(ctx)
	}()

	require.Eventually(t, func() bool {
		return client.PubSubNumSub(ctx, transport.channel).Val()[transport.channel] > 0
	}, 2*time.Second, 5*time.Millisecond, "subscriber must connect before publish")

	err = client.Publish(ctx, transport.channel, string(payload)).Err()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		transport.mutex.RLock()
		defer transport.mutex.RUnlock()
		return transport.lastEventID == "urn:uuid:foreign"
	}, 2*time.Second, 5*time.Millisecond, "pub/sub messages from other instances must update lastEventID")

	cancel()
	<-done
}

func TestSubscribePubSub_InvalidJSONDoesNotCrash(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		done <- transport.subscribePubSub(ctx)
	}()

	require.Eventually(t, func() bool {
		return client.PubSubNumSub(ctx, transport.channel).Val()[transport.channel] > 0
	}, 2*time.Second, 5*time.Millisecond, "subscriber must connect before publish")

	// Publish garbage. Must not panic, just log and continue.
	err := client.Publish(ctx, transport.channel, "{{invalid json}}").Err()
	require.NoError(t, err)

	cancel()
	<-done

	transport.mutex.RLock()
	defer transport.mutex.RUnlock()

	assert.Equal(t, mercure.EarliestLastEventID, transport.lastEventID,
		"invalid pub/sub message must not update lastEventID")
}

func TestSubscribePubSub_NilUpdateDoesNotCrash(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		done <- transport.subscribePubSub(ctx)
	}()

	require.Eventually(t, func() bool {
		return client.PubSubNumSub(ctx, transport.channel).Val()[transport.channel] > 0
	}, 2*time.Second, 5*time.Millisecond, "subscriber must connect before publish")

	// Valid JSON from a foreign instance but without an update payload, which would
	// otherwise nil-deref message.Update.ID and crash the listener.
	for _, payload := range []string{`{"instance_id":"other-instance"}`, `{"instance_id":"other-instance","update":null}`} {
		require.NoError(t, client.Publish(ctx, transport.channel, payload).Err())
	}

	// Publish a valid message afterwards. Its delivery proves the listener survived.
	validMessage := pubsubMessage{
		InstanceID: "other-instance",
		Update:     &mercure.Update{Event: mercure.Event{ID: "urn:uuid:after-nil"}, Topics: []string{"/test/topic"}},
	}
	validPayload, err := json.Marshal(validMessage)
	require.NoError(t, err)
	require.NoError(t, client.Publish(ctx, transport.channel, string(validPayload)).Err())

	require.Eventually(t, func() bool {
		transport.mutex.RLock()
		defer transport.mutex.RUnlock()
		return transport.lastEventID == "urn:uuid:after-nil"
	}, 2*time.Second, 5*time.Millisecond, "listener must survive nil-update messages and keep processing")

	cancel()
	<-done
}

func TestInterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ mercure.Transport = (*RedisTransport)(nil)
	var _ mercure.TransportSubscribers = (*RedisTransport)(nil)
}

func TestDispatch_PopulatesIndex(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("indexed-event")
	require.NoError(t, transport.Dispatch(ctx, update))

	streamID, err := client.HGet(ctx, "mercure:index", update.ID).Result()
	require.NoError(t, err)
	assert.NotEmpty(t, streamID, "index must map event UUID to stream ID")

	// Verify the stream ID actually exists in the stream.
	entries, err := client.XRangeN(ctx, "mercure", streamID, streamID, 1).Result()
	require.NoError(t, err)
	assert.Len(t, entries, 1, "index must point to a valid stream entry")
}

func TestResolveStreamCursor_IndexHit(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	update := newUpdate("cursor-test")
	require.NoError(t, transport.Dispatch(ctx, update))

	cursor, found := transport.resolveStreamCursor(ctx, update.ID)
	assert.True(t, found, "resolveStreamCursor must find indexed events")
	assert.True(t, strings.HasPrefix(cursor, "("), "cursor must be exclusive (prefixed with '(')")
}

func TestResolveStreamCursor_IndexMiss(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	cursor, found := transport.resolveStreamCursor(ctx, "urn:uuid:nonexistent")
	assert.False(t, found)
	assert.Equal(t, "-", cursor, "must return '-' for unknown events")
}

func TestResolveStreamCursor_StaleIndexEntry(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	// Manually add a stale index entry pointing to a non-existent stream ID.
	require.NoError(t, client.HSet(ctx, "mercure:index", "urn:uuid:stale", "99999-0").Err())

	cursor, found := transport.resolveStreamCursor(ctx, "urn:uuid:stale")
	assert.False(t, found, "must not resolve a stale index entry")
	assert.Equal(t, "-", cursor)

	// Stale entry should be cleaned up.
	exists, err := client.HExists(ctx, "mercure:index", "urn:uuid:stale").Result()
	require.NoError(t, err)
	assert.False(t, exists, "stale index entry must be removed")
}

func TestCurrentLastEventID(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	assert.Equal(t, mercure.EarliestLastEventID, transport.currentLastEventID())

	update := newUpdate("event")
	require.NoError(t, transport.Dispatch(context.Background(), update))

	assert.Equal(t, update.ID, transport.currentLastEventID())
}

func TestHistoryReplayCursor_EarliestReplaysFromStart(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	cursor, replay := transport.historyReplayCursor(context.Background(), mercure.EarliestLastEventID)
	assert.True(t, replay, "the earliest sentinel must replay the full stream")
	assert.Equal(t, "-", cursor)
}

func TestHistoryReplayCursor_IndexedIDReplaysAfterAnchor(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)
	ctx := context.Background()

	anchor := newUpdate("anchor")
	require.NoError(t, transport.Dispatch(ctx, anchor))

	cursor, replay := transport.historyReplayCursor(ctx, anchor.ID)
	assert.True(t, replay)
	assert.True(t, strings.HasPrefix(cursor, "("), "cursor must be exclusive so the anchor itself is not re-sent")
}

// An unanchorable client-supplied ID must refuse replay, not fall back to a scan.
func TestHistoryReplayCursor_UnanchorableIDRefusesReplay(t *testing.T) {
	t.Parallel()

	transport, _, _ := newTestTransport(t)

	cursor, replay := transport.historyReplayCursor(context.Background(), "urn:uuid:unknown-client-id")
	assert.False(t, replay, "an unanchorable Last-Event-ID must never be linear-scanned")
	assert.Empty(t, cursor)
}

func TestCleanupIndex_RemovesStaleEntries(t *testing.T) {
	t.Parallel()

	transport, client, _ := newTestTransport(t)
	ctx := context.Background()

	// Dispatch several events so the stream has entries.
	updates := seedStream(t, transport, 5)

	// Manually add stale index entries pointing to stream IDs older than any in the stream.
	require.NoError(t, client.HSet(ctx, "mercure:index", "urn:uuid:old-1", "0-1").Err())
	require.NoError(t, client.HSet(ctx, "mercure:index", "urn:uuid:old-2", "0-2").Err())

	transport.cleanupIndex(ctx)

	// Stale entries should be gone.
	exists1, err := client.HExists(ctx, "mercure:index", "urn:uuid:old-1").Result()
	require.NoError(t, err)
	assert.False(t, exists1, "stale index entries must be removed during cleanup")

	exists2, err := client.HExists(ctx, "mercure:index", "urn:uuid:old-2").Result()
	require.NoError(t, err)
	assert.False(t, exists2)

	// Valid entries should remain.
	for _, u := range updates {
		exists, err := client.HExists(ctx, "mercure:index", u.ID).Result()
		require.NoError(t, err)
		assert.True(t, exists, "valid index entry for %s must survive cleanup", u.ID)
	}
}

func TestCompareStreamIDs(t *testing.T) {
	t.Parallel()

	assert.Equal(t, -1, compareStreamIDs("100-0", "200-0"))
	assert.Equal(t, 1, compareStreamIDs("200-0", "100-0"))
	assert.Equal(t, 0, compareStreamIDs("100-0", "100-0"))
	assert.Equal(t, -1, compareStreamIDs("100-0", "100-1"))
	assert.Equal(t, 1, compareStreamIDs("100-1", "100-0"))
}
