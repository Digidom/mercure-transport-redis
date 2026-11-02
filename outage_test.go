// SPDX-License-Identifier: AGPL-3.0-or-later

package redistransport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dunglas/mercure"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// cuttableProxy is a TCP proxy that can simulate a network partition between one
// hub and Redis: Cut() drops every open connection and refuses new ones until
// Restore(). It exercises the real go-redis reconnect path, which a direct call to
// an internal replay function cannot.
type cuttableProxy struct {
	listener net.Listener
	target   string

	mutex sync.Mutex
	cut   bool
	conns []net.Conn
}

func newCuttableProxy(t *testing.T, target string) *cuttableProxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	proxy := &cuttableProxy{listener: listener, target: target}
	t.Cleanup(func() {
		listener.Close()
		proxy.Cut()
	})

	go proxy.serve()

	return proxy
}

func (p *cuttableProxy) Addr() string { return p.listener.Addr().String() }

func (p *cuttableProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mutex.Lock()
		if p.cut {
			p.mutex.Unlock()
			client.Close()

			continue
		}

		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mutex.Unlock()
			client.Close()

			continue
		}

		p.conns = append(p.conns, client, upstream)
		p.mutex.Unlock()

		go pipe(client, upstream)
		go pipe(upstream, client)
	}
}

func pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	dst.Close()
	src.Close()
}

func (p *cuttableProxy) Cut() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.cut = true
	for _, conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
}

func (p *cuttableProxy) Restore() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.cut = false
}

func newRunningTransport(t *testing.T, addr string) *RedisTransport {
	t.Helper()

	transport, err := NewRedisTransport(mercure.NewSubscriberList(1000), slog.Default(), "redis://"+addr, "mercure", 1000, 1000)
	require.NoError(t, err)
	t.Cleanup(func() { transport.Close(context.Background()) })

	return transport
}

// sseCollector subscribes to /test/topic on a hub backed by transport and records
// every "data:" line it receives.
func sseCollector(t *testing.T, transport *RedisTransport) func() []string {
	t.Helper()

	server := newTestHubServer(t, transport)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/.well-known/mercure?match=/test/topic", nil)
	require.NoError(t, err)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)

	var (
		mutex    sync.Mutex
		received []string
	)

	go func() {
		defer response.Body.Close()

		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
				mutex.Lock()
				received = append(received, data)
				mutex.Unlock()
			}
		}
	}()

	return func() []string {
		mutex.Lock()
		defer mutex.Unlock()

		return append([]string(nil), received...)
	}
}

func waitForPubSubSubscribers(t *testing.T, client *redis.Client, channel string, count int64) {
	t.Helper()

	require.Eventually(t, func() bool {
		return client.PubSubNumSub(context.Background(), channel).Val()[channel] >= count
	}, 5*time.Second, 10*time.Millisecond, "pub/sub listeners must be connected")
}

func TestOutage_CrossInstanceDelivery(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	inspector := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { inspector.Close() })

	proxy := newCuttableProxy(t, server.Addr())
	hubA := newRunningTransport(t, proxy.Addr())
	hubB := newRunningTransport(t, server.Addr())

	receivedOnA := sseCollector(t, hubA)
	waitForPubSubSubscribers(t, inspector, "mercure:pubsub", 2)

	require.NoError(t, hubB.Dispatch(context.Background(), newUpdate("before-cut")))
	require.Eventually(t, func() bool { return contains(receivedOnA(), "before-cut") },
		5*time.Second, 10*time.Millisecond, "baseline: live cross-instance delivery must work")

	// Partition hub A from Redis while hub B keeps publishing.
	proxy.Cut()
	require.Eventually(t, func() bool {
		return inspector.PubSubNumSub(context.Background(), "mercure:pubsub").Val()["mercure:pubsub"] == 1
	}, 5*time.Second, 10*time.Millisecond, "hub A's pub/sub connection must be gone")

	require.NoError(t, hubB.Dispatch(context.Background(), newUpdate("during-cut")))

	time.Sleep(300 * time.Millisecond)
	proxy.Restore()

	require.Eventually(t, func() bool { return contains(receivedOnA(), "during-cut") },
		10*time.Second, 20*time.Millisecond,
		"an update published while hub A was partitioned must reach A's subscribers once Redis is back; got %v", receivedOnA())

	require.NoError(t, hubB.Dispatch(context.Background(), newUpdate("after-restore")))
	require.Eventually(t, func() bool { return contains(receivedOnA(), "after-restore") },
		10*time.Second, 20*time.Millisecond, "live delivery must resume after the partition")

	require.Equal(t, 1, count(receivedOnA(), "during-cut"), "the missed update must be delivered exactly once")
	require.Equal(t, 1, count(receivedOnA(), "after-restore"), "live updates must not be delivered twice")
}

func TestOutage_LocalDeliveryWhenRedisIsDown(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	proxy := newCuttableProxy(t, server.Addr())
	hub, err := NewRedisTransport(mercure.NewSubscriberList(1000), slog.Default(),
		"redis://"+proxy.Addr()+"?dial_timeout=200ms&read_timeout=200ms&write_timeout=200ms&max_retries=0",
		"mercure", 1000, 1000)
	require.NoError(t, err)
	t.Cleanup(func() { hub.Close(context.Background()) })

	received := sseCollector(t, hub)

	proxy.Cut()

	err = hub.Dispatch(context.Background(), newUpdate("redis-down"))
	require.NoError(t, err, "a Redis failure must not fail the publish: the update still reaches local subscribers")

	require.Eventually(t, func() bool { return contains(received(), "redis-down") },
		5*time.Second, 10*time.Millisecond, "subscribers on the publishing hub must receive the update even when Redis is down")
}

func TestOutage_DispatchOnClosedTransportStillFails(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	hub := newRunningTransport(t, server.Addr())
	require.NoError(t, hub.Close(context.Background()))

	err := hub.Dispatch(context.Background(), newUpdate("closed"))
	require.True(t, errors.Is(err, mercure.ErrClosedTransport))
}

func contains(values []string, want string) bool { return count(values, want) > 0 }

func count(values []string, want string) int {
	n := 0
	for _, value := range values {
		if value == want {
			n++
		}
	}

	return n
}
