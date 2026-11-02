// Copyright (C) 2026 desk.ly GmbH
// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
)

type RedisTransport struct {
	subscribers  *mercure.SubscriberList
	logger       *slog.Logger
	client       *redis.Client
	stream       string
	channel      string
	indexKey     string // Redis Hash mapping Mercure event UUIDs to stream IDs for O(1) lookups
	instanceID   string // Filters own Pub/Sub messages to prevent double-dispatch to local subscribers
	size         int64
	historyLimit int64 // Max stream entries read per history replay. 0 disables replay (fast-forward only)
	closed       chan struct{}
	closedOnce   sync.Once
	cancel       context.CancelFunc

	// catchUpInterval is how often the stream is polled for updates that Pub/Sub
	// did not deliver. 0 disables the catch-up loop (Pub/Sub only, best-effort).
	catchUpInterval time.Duration

	// delivered remembers the IDs of updates already handed to local subscribers,
	// so an update seen both live (Pub/Sub or local Dispatch) and by the catch-up
	// loop is delivered exactly once.
	delivered *deliveredSet

	// catchUpMutex serializes catch-up passes and guards streamCursor, the ID of
	// the last stream entry examined by the catch-up loop.
	catchUpMutex sync.Mutex
	streamCursor string

	mutex       sync.RWMutex
	lastEventID string
}

type pubsubMessage struct {
	InstanceID string          `json:"instance_id"`
	Update     *mercure.Update `json:"update"`
}

const (
	trimInterval       = 10 * time.Second
	pubsubReconnectMin = 500 * time.Millisecond
	pubsubReconnectMax = 30 * time.Second

	// DefaultCatchUpInterval bounds how late a cross-instance update can arrive
	// when its Pub/Sub message was lost (network blip, Redis failover).
	DefaultCatchUpInterval = 2 * time.Second

	// deliveredSetSize must exceed the number of updates the whole cluster can
	// publish during one catch-up interval, otherwise duplicates become possible.
	deliveredSetSize = 10000
)

// Option customizes a RedisTransport.
type Option func(*RedisTransport)

// WithCatchUpInterval sets how often the stream is polled for updates missed by
// Pub/Sub. 0 disables the catch-up loop.
func WithCatchUpInterval(interval time.Duration) Option {
	return func(t *RedisTransport) {
		t.catchUpInterval = interval
	}
}

func NewRedisTransport(
	subscriberList *mercure.SubscriberList,
	logger *slog.Logger,
	url string,
	stream string,
	size int64,
	historyLimit int64,
	options ...Option,
) (*RedisTransport, error) {
	redisOptions, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid Redis URL %q: %w", url, err)
	}

	client := redis.NewClient(redisOptions)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("unable to connect to Redis: %w", err)
	}

	if stream == "" {
		stream = "mercure"
	}

	listenCtx, listenCancel := context.WithCancel(context.Background())

	t := &RedisTransport{
		subscribers:  subscriberList,
		logger:       logger,
		client:       client,
		stream:       stream,
		channel:      stream + ":pubsub",
		indexKey:     stream + ":index",
		instanceID:   generateInstanceID(),
		size:         size,
		historyLimit: historyLimit,
		closed:       make(chan struct{}),
		cancel:       listenCancel,
		lastEventID:  mercure.EarliestLastEventID,

		catchUpInterval: DefaultCatchUpInterval,
		delivered:       newDeliveredSet(deliveredSetSize),
	}

	for _, option := range options {
		option(t)
	}

	t.lastEventID = t.fetchLastEventID()

	// Anchor the catch-up cursor BEFORE subscribing to Pub/Sub: anything published
	// between this point and the SUBSCRIBE being active is then recovered from the
	// stream instead of being lost.
	t.streamCursor = t.fetchLatestStreamID(ctx)

	go t.listenPubSub(listenCtx)
	go t.trimLoop(listenCtx)

	if t.catchUpInterval > 0 {
		go t.catchUpLoop(listenCtx)
	}

	return t, nil
}

// fetchLatestStreamID returns the ID of the newest stream entry, or "0-0" when the
// stream is empty or unreadable (the catch-up loop then starts from the beginning).
func (t *RedisTransport) fetchLatestStreamID(ctx context.Context) string {
	messages, err := t.client.XRevRangeN(ctx, t.stream, "+", "-", 1).Result()
	if err != nil || len(messages) == 0 {
		return "0-0"
	}

	return messages[0].ID
}

func generateInstanceID() string {
	randomBytes := make([]byte, 16)
	rand.Read(randomBytes) //nolint:errcheck
	return hex.EncodeToString(randomBytes)
}

func (t *RedisTransport) fetchLastEventID() string {
	messages, err := t.client.XRevRangeN(context.Background(), t.stream, "+", "-", 1).Result()
	if err != nil || len(messages) == 0 {
		return mercure.EarliestLastEventID
	}

	data, ok := messages[0].Values["data"].(string)
	if !ok {
		return mercure.EarliestLastEventID
	}

	var update mercure.Update
	if err := json.Unmarshal([]byte(data), &update); err != nil {
		return mercure.EarliestLastEventID
	}

	return update.ID
}

// listenPubSub delivers live updates from other instances.
//
// It is NOT the recovery mechanism: go-redis reconnects a PubSub transparently and
// only closes its message channel when the client itself is closed, so a network
// blip or a Redis failover silently drops the messages published meanwhile without
// this loop ever noticing. Missed updates are recovered by catchUpLoop instead.
func (t *RedisTransport) listenPubSub(ctx context.Context) {
	backoff := pubsubReconnectMin

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		wasHealthy := t.subscribePubSub(ctx)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			t.logger.LogAttrs(ctx, slog.LevelWarn, "Redis Pub/Sub disconnected, reconnecting",
				slog.Duration("backoff", backoff))
		}

		// Recover what landed in the stream while Pub/Sub was down, without waiting
		// for the next catch-up tick.
		t.catchUp(ctx)

		if wasHealthy {
			backoff = pubsubReconnectMin
		} else {
			backoff = min(backoff*2, pubsubReconnectMax)
		}
	}
}

// catchUpLoop periodically delivers stream entries that never reached this
// instance through Pub/Sub. The stream is the source of truth; Pub/Sub is only the
// low-latency path.
func (t *RedisTransport) catchUpLoop(ctx context.Context) {
	ticker := time.NewTicker(t.catchUpInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.catchUp(ctx)
		}
	}
}

// catchUp reads every stream entry after streamCursor and delivers to local
// subscribers the ones not delivered yet (by Pub/Sub or by a local Dispatch).
//
// The cursor is a Redis stream ID, which Redis assigns monotonically, so no entry
// can appear behind it: unlike an anchor on the last delivered Mercure event ID,
// it cannot skip an entry that Pub/Sub delivered out of stream order. If the
// cursor was trimmed away, XRANGE simply resumes from the oldest entry left.
func (t *RedisTransport) catchUp(ctx context.Context) {
	t.catchUpMutex.Lock()
	defer t.catchUpMutex.Unlock()

	for {
		messages, err := t.client.XRangeN(ctx, t.stream, "("+t.streamCursor, "+", historyPageSize).Result()
		if err != nil {
			if ctx.Err() == nil {
				t.logger.LogAttrs(ctx, slog.LevelWarn, "Unable to read Redis Stream for catch-up, will retry", slog.Any("error", err))
			}

			return
		}

		for _, message := range messages {
			t.streamCursor = message.ID

			data, ok := message.Values["data"].(string)
			if !ok {
				continue
			}

			var update mercure.Update
			if err := json.Unmarshal([]byte(data), &update); err != nil {
				t.logger.LogAttrs(ctx, slog.LevelError, "Unable to unmarshal update from Redis Stream during catch-up", slog.Any("error", err))

				continue
			}

			t.deliverLocally(ctx, &update)
		}

		if int64(len(messages)) < historyPageSize {
			return
		}
	}
}

// deliverLocally hands an update to the subscribers connected to this instance,
// unless it was already delivered. It is the single delivery path shared by
// Dispatch, Pub/Sub and the catch-up loop.
func (t *RedisTransport) deliverLocally(ctx context.Context, update *mercure.Update) {
	if !t.delivered.add(update.ID) {
		return
	}

	t.mutex.Lock()
	t.lastEventID = update.ID
	t.mutex.Unlock()

	for _, subscriber := range t.subscribers.MatchAny(update) {
		subscriber.Dispatch(ctx, update, false)
	}
}

// subscribePubSub returns true if at least one message was received (= healthy connection).
func (t *RedisTransport) subscribePubSub(ctx context.Context) (received bool) {
	pubsub := t.client.Subscribe(ctx, t.channel)
	defer pubsub.Close()

	// A single malformed message must not crash the long-lived listener goroutine.
	// recover keeps the transport running.
	defer func() {
		if r := recover(); r != nil {
			t.logger.LogAttrs(ctx, slog.LevelError, "Recovered from panic in Redis Pub/Sub listener", slog.Any("panic", r))
		}
	}()

	pubsubChannel := pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return received
		case redisMessage, ok := <-pubsubChannel:
			if !ok {
				return received
			}

			received = true

			var message pubsubMessage
			if err := json.Unmarshal([]byte(redisMessage.Payload), &message); err != nil {
				t.logger.LogAttrs(ctx, slog.LevelWarn, "Unable to unmarshal Redis Pub/Sub message, skipping", slog.Any("error", err))
				continue
			}

			if message.InstanceID == t.instanceID {
				continue
			}

			// The legitimate path always sets Update. A nil here means a foreign or
			// malformed PUBLISH (missing field or "update":null) that would otherwise nil-deref below.
			if message.Update == nil {
				t.logger.LogAttrs(ctx, slog.LevelWarn, "Received Redis Pub/Sub message without update payload, skipping", slog.String("instance_id", message.InstanceID))
				continue
			}

			t.deliverLocally(ctx, message.Update)
		}
	}
}

const historyPageSize int64 = 1000

func (t *RedisTransport) resolveStreamCursor(ctx context.Context, eventID string) (cursor string, found bool) {
	streamID, err := t.client.HGet(ctx, t.indexKey, eventID).Result()
	if err != nil {
		return "-", false
	}

	entries, err := t.client.XRangeN(ctx, t.stream, streamID, streamID, 1).Result()
	if err != nil || len(entries) == 0 {
		t.client.HDel(ctx, t.indexKey, eventID) //nolint:errcheck
		return "-", false
	}

	return "(" + streamID, true
}

func (t *RedisTransport) cleanupIndex(ctx context.Context) {
	oldest, err := t.client.XRangeN(ctx, t.stream, "-", "+", 1).Result()
	if err != nil || len(oldest) == 0 {
		return
	}
	oldestStreamID := oldest[0].ID

	var scanCursor uint64
	for {
		keys, nextCursor, err := t.client.HScan(ctx, t.indexKey, scanCursor, "*", 100).Result()
		if err != nil {
			return
		}

		var stale []string
		for i := 0; i+1 < len(keys); i += 2 {
			streamID := keys[i+1]
			if compareStreamIDs(streamID, oldestStreamID) < 0 {
				stale = append(stale, keys[i])
			}
		}

		if len(stale) > 0 {
			t.client.HDel(ctx, t.indexKey, stale...) //nolint:errcheck
		}

		scanCursor = nextCursor
		if scanCursor == 0 {
			break
		}
	}
}

// trimLoop bounds the stream and index on a timer. Trimming on a schedule (rather
// than per write) keeps it independent of per-instance write counts and process
// restarts, which matters because the stream is shared across all instances.
func (t *RedisTransport) trimLoop(ctx context.Context) {
	ticker := time.NewTicker(trimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.trim(ctx)
		}
	}
}

// trim caps the stream at the configured size using an exact MAXLEN (not approximate,
// so the bound holds precisely) and drops index entries for evicted events.
func (t *RedisTransport) trim(ctx context.Context) {
	if t.size <= 0 {
		return
	}

	if err := t.client.XTrimMaxLen(ctx, t.stream, t.size).Err(); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelError, "Failed to trim Redis Stream", slog.Any("error", err))
		return
	}

	t.cleanupIndex(ctx)
}

func compareStreamIDs(a, b string) int {
	aTimestamp, aSequence := parseStreamID(a)
	bTimestamp, bSequence := parseStreamID(b)

	if aTimestamp != bTimestamp {
		if aTimestamp < bTimestamp {
			return -1
		}
		return 1
	}
	if aSequence != bSequence {
		if aSequence < bSequence {
			return -1
		}
		return 1
	}
	return 0
}

func parseStreamID(id string) (int64, int64) {
	parts := strings.SplitN(id, "-", 2)
	timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
	var sequence int64
	if len(parts) > 1 {
		sequence, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return timestamp, sequence
}

func (t *RedisTransport) Dispatch(ctx context.Context, update *mercure.Update) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	update.AssignUUID()

	// Local subscribers are served first and unconditionally: Redis only carries
	// the update to the OTHER instances, so a Redis outage must degrade to
	// single-instance delivery instead of failing the publish. Delivering first
	// also marks the update as delivered before it is in the stream, so the
	// catch-up loop can never hand it out a second time.
	t.deliverLocally(ctx, update)

	if err := t.propagate(ctx, update); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelError, "Unable to propagate update through Redis, delivered to this instance only",
			slog.String("update.id", update.ID), slog.Any("error", err))
	}

	return nil
}

// propagate stores the update in the shared stream (history, catch-up) and
// announces it on Pub/Sub (low-latency path to the other instances).
func (t *RedisTransport) propagate(ctx context.Context, update *mercure.Update) error {
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	streamID, err := t.client.XAdd(ctx, &redis.XAddArgs{
		Stream: t.stream,
		Values: map[string]interface{}{"data": string(updateJSON)},
	}).Result()
	if err != nil {
		return fmt.Errorf("error adding to Redis Stream: %w", err)
	}

	// Index event UUID → stream ID for O(1) lookups in history replay.
	if err := t.client.HSet(ctx, t.indexKey, update.ID, streamID).Err(); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "Unable to index update, history replay from it will fast-forward",
			slog.String("update.id", update.ID), slog.Any("error", err))
	}

	pubsubPayload, err := json.Marshal(pubsubMessage{InstanceID: t.instanceID, Update: update})
	if err != nil {
		return fmt.Errorf("error when marshaling pub/sub message: %w", err)
	}

	// Pub/Sub is best-effort: the update is in the stream, so the other instances
	// pick it up on their next catch-up pass if this message is lost.
	if err := t.client.Publish(ctx, t.channel, string(pubsubPayload)).Err(); err != nil {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "Failed to publish to Redis Pub/Sub, other instances will catch up from the stream",
			slog.Any("error", err))
	}

	return nil
}

func (t *RedisTransport) AddSubscriber(ctx context.Context, subscriber *mercure.LocalSubscriber) error {
	select {
	case <-t.closed:
		return mercure.ErrClosedTransport
	default:
	}

	t.subscribers.Add(subscriber)

	// Guards a race where Close() between the first select and Add() would miss this
	// subscriber during its disconnect walk, leaving a zombie SSE connection open forever.
	select {
	case <-t.closed:
		t.subscribers.Remove(subscriber)
		subscriber.Disconnect()
		return mercure.ErrClosedTransport
	default:
	}

	// Also true for an empty Last-Event-ID, which the hub still waits on.
	if subscriber.RequestLastEventIDSet {
		if err := t.dispatchHistory(ctx, subscriber); err != nil {
			return err
		}
	}

	subscriber.Ready(ctx)

	return nil
}

// currentLastEventID returns the latest known event ID from memory (no Redis round-trip).
func (t *RedisTransport) currentLastEventID() string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.lastEventID
}

// historyReplayCursor returns the exclusive stream cursor to replay from for a
// reconnecting subscriber's requested Last-Event-ID.
//
// replay is false when the ID can't be anchored (trimmed or unknown). Callers MUST NOT
// then scan the stream, because Last-Event-ID is client-controlled and an arbitrary ID
// would make each reconnect read the entire stream.
func (t *RedisTransport) historyReplayCursor(ctx context.Context, requestedID string) (cursor string, replay bool) {
	if requestedID == mercure.EarliestLastEventID {
		return "-", true
	}

	resolvedCursor, found := t.resolveStreamCursor(ctx, requestedID)
	if !found {
		return "", false
	}

	return resolvedCursor, true
}

// dispatchHistory replays missed events to a reconnecting SSE subscriber, starting
// just after its requested Last-Event-ID.
//
// Reads are capped at historyLimit entries (0 disables replay entirely). The stream
// is shared across all topics with no per-topic filtering, so an uncapped replay
// would read the whole stream on every reconnect. Exceeding the budget (or an
// unanchorable Last-Event-ID) fast-forwards the subscriber to live instead.
func (t *RedisTransport) dispatchHistory(ctx context.Context, subscriber *mercure.LocalSubscriber) error {
	if subscriber.RequestLastEventID == "" {
		subscriber.HistoryDispatched(mercure.EarliestLastEventID)

		return nil
	}

	if t.historyLimit == 0 {
		subscriber.HistoryDispatched(t.currentLastEventID())

		return nil
	}

	cursor, replay := t.historyReplayCursor(ctx, subscriber.RequestLastEventID)
	if !replay {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "Requested LastEventID not found in stream index, skipping history replay",
			slog.String("requestedID", subscriber.RequestLastEventID))
		subscriber.HistoryDispatched(t.currentLastEventID())

		return nil
	}

	responseLastEventID := subscriber.RequestLastEventID
	var scanned int64

	for {
		pageSize := historyPageSize
		if remaining := t.historyLimit - scanned; remaining < pageSize {
			pageSize = remaining
		}

		messages, err := t.client.XRangeN(ctx, t.stream, cursor, "+", pageSize).Result()
		if err != nil {
			return fmt.Errorf("error reading Redis Stream history: %w", err)
		}

		if len(messages) == 0 {
			break
		}

		for _, message := range messages {
			data, ok := message.Values["data"].(string)
			if !ok {
				continue
			}

			var update mercure.Update
			if err := json.Unmarshal([]byte(data), &update); err != nil {
				t.logger.LogAttrs(ctx, slog.LevelError, "Unable to unmarshal update from Redis Stream", slog.Any("error", err))
				continue
			}

			responseLastEventID = update.ID
			if subscriber.Match(&update) && !subscriber.Dispatch(ctx, &update, true) {
				subscriber.HistoryDispatched(responseLastEventID)
				return nil
			}
		}

		scanned += int64(len(messages))

		if int64(len(messages)) < pageSize {
			break
		}

		if scanned >= t.historyLimit {
			t.logger.LogAttrs(ctx, slog.LevelWarn, "History replay limit reached, fast-forwarding subscriber",
				slog.Int64("limit", t.historyLimit),
				slog.String("requestedID", subscriber.RequestLastEventID))
			subscriber.HistoryDispatched(responseLastEventID)

			return nil
		}

		cursor = "(" + messages[len(messages)-1].ID
	}

	subscriber.HistoryDispatched(responseLastEventID)

	return nil
}

func (t *RedisTransport) RemoveSubscriber(_ context.Context, subscriber *mercure.LocalSubscriber) error {
	t.subscribers.Remove(subscriber)
	return nil
}

func (t *RedisTransport) GetSubscribers(_ context.Context) (string, []*mercure.Subscriber, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	var subscribers []*mercure.Subscriber
	t.subscribers.Walk(0, func(subscriber *mercure.LocalSubscriber) bool {
		subscribers = append(subscribers, &subscriber.Subscriber)
		return true
	})

	return t.lastEventID, subscribers, nil
}

func (t *RedisTransport) Close(_ context.Context) (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)
		t.cancel()

		t.subscribers.Walk(0, func(subscriber *mercure.LocalSubscriber) bool {
			subscriber.Disconnect()
			return true
		})

		err = t.client.Close()
	})

	if err != nil {
		return fmt.Errorf("unable to close Redis connection: %w", err)
	}

	return nil
}

var (
	_ mercure.Transport            = (*RedisTransport)(nil)
	_ mercure.TransportSubscribers = (*RedisTransport)(nil)
)
