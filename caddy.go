// Copyright (C) 2026 desk.ly GmbH
// SPDX-License-Identifier: AGPL-3.0-or-later

// Caddy module wrapper that registers the Redis transport as http.handlers.mercure.redis.
// Enables `transport redis { url ...; stream ...; size ... }` in Caddyfile.
package redistransport

import (
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/dunglas/mercure"
	mercurecaddy "github.com/dunglas/mercure/caddy"
)

// defaultHistoryLimit applies when the Caddyfile omits the history_limit directive.
const defaultHistoryLimit int64 = 1000

func init() {
	caddy.RegisterModule(Redis{})
}

type Redis struct {
	URL          string `json:"url,omitempty"`
	Stream       string `json:"stream,omitempty"`
	Size         int64  `json:"size,omitempty"`
	HistoryLimit *int64 `json:"history_limit,omitempty"` // nil = default, 0 = replay disabled
	// nil = DefaultCatchUpInterval, 0 = catch-up disabled (Pub/Sub only, best-effort)
	CatchUpInterval *caddy.Duration `json:"catchup_interval,omitempty"`

	transport    *RedisTransport
	transportKey redisTransportKey
}

// Hub name as Mercure requires; the full config so a reload applies changes.
type redisTransportKey struct {
	hub, url, stream   string
	size, historyLimit int64
	catchUpInterval    time.Duration
}

func (Redis) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.mercure.redis",
		New: func() caddy.Module { return new(Redis) },
	}
}

func (r *Redis) GetTransport() mercure.Transport { //nolint:ireturn
	return r.transport
}

func (r *Redis) Provision(ctx caddy.Context) error {
	historyLimit := defaultHistoryLimit
	if r.HistoryLimit != nil {
		historyLimit = *r.HistoryLimit
	}

	catchUpInterval := DefaultCatchUpInterval
	if r.CatchUpInterval != nil {
		catchUpInterval = time.Duration(*r.CatchUpInterval)
	}

	hubName, _ := ctx.Value(mercurecaddy.HubNameContextKey).(string)
	r.transportKey = redisTransportKey{
		hub: hubName, url: r.URL, stream: r.Stream, size: r.Size,
		historyLimit: historyLimit, catchUpInterval: catchUpInterval,
	}

	destructor, _, err := mercurecaddy.TransportUsagePool.LoadOrNew(r.transportKey, func() (caddy.Destructor, error) {
		transport, err := NewRedisTransport(
			mercure.NewSubscriberList(ctx.Value(mercurecaddy.SubscriberListCacheSizeContextKey).(int)),
			ctx.Slogger(),
			r.URL,
			r.Stream,
			r.Size,
			historyLimit,
			WithCatchUpInterval(catchUpInterval),
		)

		if err != nil {
			return nil, err
		}

		return mercurecaddy.TransportDestructor[*RedisTransport]{Transport: transport}, nil
	})
	if err != nil {
		return err //nolint:wrapcheck
	}

	r.transport = destructor.(mercurecaddy.TransportDestructor[*RedisTransport]).Transport

	return nil
}

func (r *Redis) Cleanup() error {
	_, err := mercurecaddy.TransportUsagePool.Delete(r.transportKey)
	return err //nolint:wrapcheck
}

func (r *Redis) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		for d.NextBlock(0) {
			switch d.Val() {
			case "url":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.URL = d.Val()

			case "stream":
				if !d.NextArg() {
					return d.ArgErr()
				}
				r.Stream = d.Val()

			case "size":
				if !d.NextArg() {
					return d.ArgErr()
				}
				streamSize, err := strconv.ParseInt(d.Val(), 10, 64)
				if err != nil {
					return d.WrapErr(err)
				}
				r.Size = streamSize

			case "history_limit":
				if !d.NextArg() {
					return d.ArgErr()
				}
				historyLimit, err := strconv.ParseInt(d.Val(), 10, 64)
				if err != nil {
					return d.WrapErr(err)
				}
				if historyLimit < 0 {
					return d.Errf("history_limit must be >= 0, got %d", historyLimit)
				}
				r.HistoryLimit = &historyLimit

			case "catchup_interval":
				if !d.NextArg() {
					return d.ArgErr()
				}
				interval, err := caddy.ParseDuration(d.Val())
				if err != nil {
					return d.WrapErr(err)
				}
				if interval < 0 {
					return d.Errf("catchup_interval must be >= 0, got %s", d.Val())
				}
				catchUpInterval := caddy.Duration(interval)
				r.CatchUpInterval = &catchUpInterval

			default:
				return d.Errf("unknown redis transport directive: %s", d.Val())
			}
		}
	}

	return nil
}

var (
	_ caddy.Provisioner     = (*Redis)(nil)
	_ caddy.CleanerUpper    = (*Redis)(nil)
	_ caddyfile.Unmarshaler = (*Redis)(nil)
)
