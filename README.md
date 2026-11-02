# Mercure Redis Transport

A [Redis](https://redis.io) transport for the [Mercure](https://mercure.rocks) hub, packaged as a
[Caddy](https://caddyserver.com) module.

The open-source Mercure hub ships single-process transports (`local`, `bolt`), so a horizontally
scaled hub cannot deliver an update published on one instance to subscribers connected to another.
This module fixes that. Redis Streams hold the event history and are the source of truth, Redis
Pub/Sub fans out to the other instances with low latency, a periodic catch-up pass over the stream
delivers whatever Pub/Sub missed, and a UUID → stream-ID index keeps `Last-Event-ID` replay O(1).

Requires Redis 5.0 or newer, for Redis Streams. Supported Go, Caddy, and Mercure versions are the
ones declared in [`go.mod`](./go.mod). The configuration surface is stable as of 1.0.0 and follows
[semantic versioning](https://semver.org), so breaking changes wait for a major release.

## Install

Build Caddy with [xcaddy](https://github.com/caddyserver/xcaddy):

```bash
xcaddy build \
    --with github.com/dunglas/mercure/caddy \
    --with github.com/desk-ly/mercure-transport-redis
```

## Configure

```caddy
localhost {
    route {
        mercure {
            transport redis {
                url {$REDIS_URL}
                stream mercure
                size 10000
                history_limit 1000
                catchup_interval 2s
            }

            issuer https://example.com {
                publisher {
                    jwt {$MERCURE_PUBLISHER_JWT_KEY}
                }
                subscriber {
                    jwt {$MERCURE_SUBSCRIBER_JWT_KEY}
                }
            }
        }

        respond 404
    }
}
```

| Parameter | Default | Description |
|---|---|---|
| `url` | *(required)* | Redis URL, e.g. `redis://host:6379/0` or `rediss://` for TLS. Parsed by [go-redis `ParseURL`](https://pkg.go.dev/github.com/redis/go-redis/v9#ParseURL), so it also carries credentials and DB index. |
| `stream` | `mercure` | Stream key. Also derives the Pub/Sub channel (`<stream>:pubsub`) and index hash (`<stream>:index`). |
| `size` | `0` | Max stream entries, enforced by a background `XTRIM` every 10s. **`0` means no trimming, so the stream grows without bound.** Set this. |
| `history_limit` | `1000` | Max entries read per SSE history replay. `0` disables replay, so subscribers fast-forward to live. |
| `catchup_interval` | `2s` | How often each instance reads the stream for updates its Pub/Sub connection missed (network blip, failover). It bounds how late such an update arrives. `0` disables it, leaving cross-instance delivery best-effort. |

Set client timeouts in the URL, e.g. `redis://host:6379/0?dial_timeout=1s&read_timeout=1s&write_timeout=1s&max_retries=1`.
The go-redis defaults (5s dial, 3s read, 3 retries) can hold a publish request for several seconds
when Redis stops answering.

Size the stream to cover your longest expected client disconnect at peak event rate. There is no TTL
and no eviction fallback, so `size` is the only bound on Redis memory. `Last-Event-ID` is
client-controlled, so `history_limit` caps what a single reconnect can cost. Set it to `0` if your
clients re-sync through your own API on connect.

## Security

The transport trusts its Redis. Anything that can write to `<stream>:pubsub` can inject updates
without a publisher JWT, and anything that can read the stream sees every update in plain text.
Subscriber authorization still applies, so an injected update only reaches subscribers whose topic
selectors already match it, but its topics and content are attacker-controlled.

Run Redis with AUTH/ACLs and TLS, and do not share the instance with untrusted workloads.

## Limitations

- All topics share one stream, and cross-instance replay filters locally rather than at the stream
  level.
- Named hubs on the same `stream` share updates and history. Give each hub its own `stream`.
- `history_limit` bounds the client-facing path only. Internal gap recovery after a Pub/Sub outage may
  dispatch up to `size` events at once. The anchor is server-controlled, so this is not
  client-triggerable.
- When Redis is unreachable, a publish still succeeds and reaches the subscribers of the instance it
  was published on. It never reaches the other instances, because it never made it into the stream.
  The failure is logged at error level.
- Pub/Sub delivery is best-effort. go-redis reconnects a Pub/Sub connection transparently and drops
  what was published meanwhile, so consistency comes from the catch-up pass over the stream, not from
  the channel. An update whose Pub/Sub message was lost reaches remote subscribers up to
  `catchup_interval` late.
- Duplicate suppression remembers the last 10,000 delivered update IDs per instance. Publishing more
  than that across the cluster within one `catchup_interval` can deliver an update twice.

## Development

`make test` runs the suite in Docker, so no local Go toolchain is required. Without Docker,
`go test -race -count=1 ./...` does the same. Tests run against
[miniredis](https://github.com/alicebob/miniredis), so no Redis instance is needed.

We built this at [desk.ly](https://desk.ly) and share it under AGPL-3.0 so that anyone running the hub
has the full source. It is maintained around our own needs rather than as a community project, so we
don't take external contributions or offer support. You are free to fork it and take it wherever you
need.

## License

Copyright © 2026 desk.ly GmbH. Licensed under [AGPL-3.0-or-later](./LICENSE).

This module links against the [Mercure hub](https://github.com/dunglas/mercure), which is AGPL-3.0.
A Caddy binary built with it is therefore a combined AGPL-3.0 work. If you run it as a network
service, AGPL-3.0 §13 requires you to offer its complete corresponding source to your users.
