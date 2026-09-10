# Cluster discovery (`connect_urls`), failover, and lame duck mode

## Context

`Basis\Nats\Client` can only ever talk to one endpoint. `Configuration` exposes a single
`host`/`port` pair, and [Connection.php:227](src/Connection.php#L227) is the *only* place
in `src/` that turns it into a DSN:

```php
$dsn = "$config->host:$config->port";
```

Reconnection ([Connection::processException](src/Connection.php#L321-L366)) loops on that
same address forever. So:

- **No cluster failover.** If the configured server dies, the client retries the dead
  address until `maxReconnectAttempts` is exhausted, even when other cluster members are
  healthy.
- **No topology discovery.** `Info::$connect_urls` is *declared*
  ([Info.php:20](src/Message/Info.php#L20)) but never read.
- **No lame duck handling.** `Info::$ldm` is *declared*
  ([Info.php:26](src/Message/Info.php#L26)) but never read. During a rolling upgrade
  (`nats-server --signal ldm`) the client holds its connection until the draining server
  hard-closes it, then reconnects straight back to that same draining server.
- **The client never opts in to async INFO.** `Connect::$protocol`
  ([Connect.php:18](src/Message/Connect.php#L18)) is declared but never populated, so
  CONNECT goes out with proto 0. nats-server only pushes async INFO topology updates to
  clients advertising `protocol >= 1` — so today the client could not even *receive* an
  updated `connect_urls` or an `ldm` flag mid-connection. **Nothing else here works
  without that.**
- [README.md:442](README.md#L442) documents a `servers` option that does not exist ("only
  used if `servers` is not specified") — leftover from the nats.js docs. It is currently
  a documentation lie; this change makes it true.

Intended outcome: a server pool seeded from explicit config, grown from what the cluster
advertises, rotated on reconnect with a per-server attempt budget, and an `ldm`
notification the app can act on — following nats.go semantics.

## How the reference clients behave

Researched against `nats-io/nats.go` @ v1.53.1, `nats-io/nats.py` main (both legacy
`nats-py` and the `nats-core` v2 rewrite), and `nats-io/nats.js` main.

### Discovery — `connect_urls`

Advertised entries are bare `host:port` (IPv6 via Go's `net.JoinHostPort`, so `[::1]:4222`).
nats.go `processInfo()` (nats.go:4185-4276) is the model:

1. **Replace the whole info struct first**, then touch the pool.
2. **Empty or absent `connect_urls` ⇒ no removals.** Deliberate: a server with
   `no_advertise`, or an older server, must not wipe the pool.
3. Build a set from the advertised URLs, then walk the pool once, deleting each entry's
   address from that set as it goes. This does double duty — it diffs *and* stops an
   explicitly-configured URL that also appears in `connect_urls` from being re-added as an
   implicit duplicate.
4. **Remove only if all three hold**: implicit, not the current server, absent from the new
   set. Explicitly configured servers are never removed.
5. New entries appended as implicit, then shuffle **from index 1**, preserving element 0.
6. `hasNew` — and the discovered-servers callback — is computed against a never-pruned
   "ever seen" set (nats.go never calls `delete(nc.urls, …)`). A server that flaps out of
   and back into `connect_urls` re-enters the pool but does **not** re-fire the callback.
7. Callbacks are suppressed during the initial connect (`!nc.initc`).
8. Errors are swallowed — a malformed advertised URL is silently skipped.

nats.py legacy has two defects worth *not* copying: it dedups on `uri.netloc`, which
includes userinfo (so a seed of `nats://user:pass@10.0.0.1:4222` never matches the
advertised `10.0.0.1:4222` and the server is added twice), and it doesn't check the batch
it is accumulating, so a URL repeated inside one INFO is added twice. It also never removes.

### `tlsName` — why the current hostname is copied onto discovered servers

nats.go `parseServerURL` (nats.go:2229-2247) + `createConn` (nats.go:2545-2553). Clusters
advertise `IP:port`, not DNS names. Connect to `tls://nats.example.com:4222`, get told
about `10.0.0.7:4222`, and verifying against `10.0.0.7` fails — the cert carries
`nats.example.com` and typically no IP SAN. So when the *current* host is not an IP and
the *discovered* one is, the implicit entry records `tlsName = current hostname` and uses
it as `ServerName`. An explicit user-supplied `ServerName` always wins. nats.py legacy has
the identical mechanism (`srv.tls_name`); nats-core v2 dropped it.

### Reconnect iteration

- `selectNextServer` (nats.go:2082-2103): splice current out; re-append to the **tail** if
  `Reconnects < MaxReconnect` (or `MaxReconnect < 0`), else **drop permanently**. Empty
  pool ⇒ `ErrNoServers`.
- `MaxReconnect` (default 60) is **per server**, reset to 0 on a successful connect.
- `ReconnectWait + jitter` is slept **once per full pass over the pool**, not per attempt
  (`doSleep = i+1 >= len(nc.srvPool)`). Attempts within a pass are back-to-back.
- The initial connect walks by index with **no delay**, re-evaluating `len(nc.srvPool)`
  each iteration because the first INFO can grow the pool mid-loop.

### Lame duck mode — notification only, in all three clients

This was the open question. **No official client reconnects on `ldm`.**

- **nats.go**: the only two reads of `LameDuckMode` are the two branches of `processInfo`
  (nats.go:4204-4205, :4269-4273), each doing exactly
  `nc.ach.push(func(){ lameDuckModeHandler(nc) })`. No reconnect, no delay, no status
  change, no pool removal — the draining server *is* the current server, which the removal
  loop explicitly skips. There is no `ErrLameDuckMode` and no
  `LameDuckModeStabilizationDelay`; I searched the org, both are zero hits. The client
  waits for the server to close (nats-server drains over `lame_duck_duration`, default
  2 min), then takes the normal reconnect path — where `selectNextServer` has rotated the
  drained server to the tail, so it is tried last. `TestLameDuckMode` asserts only that the
  handler fired.
- **nats.py**: `lame_duck_mode_cb`, no auto-reconnect; its test asserts `nc.is_connected`
  is still true afterwards. Documented idiom: `await nc.force_reconnect()` *inside* the
  callback.
- **nats.js**: dispatches a `{type: "ldm"}` status event (core/src/protocol.ts:893-899).
- nats.go and nats-core v2 both gate the handler on "not the initial connect"; v2 adds an
  edge-triggered latch so a repeated `ldm: true` fires once.

**Decision:** match them. Mark the entry draining, fire an optional handler, keep the
connection. Add `forceReconnect()` so the app can migrate. This matters more here than in
an async client: PHP has no callback loop, so the handler only runs when the app calls
`process()`.

### Credentials on discovered servers

nats.go inherits the current URL's userinfo onto implicit entries (nats.go:2231-2236) —
necessary because advertised URLs are bare `host:port`, so without it password auth breaks
on failover. In `connectProto` (nats.go:3053-3080) URL userinfo **overrides**
`Options.User/Password/Token/Nkey` and the two are mutually exclusive; username with no
password means token. JWT/nkey signing is option-level and re-signs the latest nonce on
every connect, so it works on discovered servers with no userinfo at all.

## Design

Naming and semantics follow nats.go throughout.

### Prerequisite bug fixes (must land first — the feature is broken without them)

A design review turned up five existing defects that this feature would either trip over
or amplify. Each is independently defensible and belongs in its own commit before any new
behaviour.

1. **`Connect::$protocol` is `string`.** With `declare(strict_types=1)` in
   [Prototype.php](src/Message/Prototype.php), assigning int `1` throws
   `TypeError: Cannot assign int to property`. Emitting `'1'` instead is worse — nats-server
   unmarshals CONNECT into `Protocol int`, so `"protocol":"1"` fails and it replies
   `-ERR 'Invalid Connect Options'` and closes. Change to `public int $protocol`. No test
   asserts the CONNECT JSON. `Connect::$tls_required` is mistyped the same way.
2. **`getOptions()` emits a null password.** [Configuration.php:75-77](src/Configuration.php#L75-L77)
   sets `$options['pass']` unconditionally whenever `user !== null`, but `pass` may be
   null, and `Connect::$pass` is `string` ⇒ `TypeError`. Rare today; per-server userinfo
   (`nats://user@host:4222`) makes it the common case.
3. **Control-line truncation** — see its own section below.
4. **The handshake is re-entrant.** `init()` writes through the self-healing
   `sendMessage()` and reads through the self-healing `getMessage()`, both of which route
   failures into `processException()` → `init()`. Trace a CONNECT-write failure: the inner
   call reconnects to server B, sends CONNECT_B, replays subscriptions — then control
   returns to the `catch` at [Connection.php:189](src/Connection.php#L189), which re-renders
   the *outer* CONNECT_A and, because `$total` is never reset, writes a **suffix of server
   A's CONNECT onto server B's already-connected socket**. The read case is as bad: the
   inner call fully connects to B, then the outer `init()` sees `null` and throws
   `"Timeout waiting for message from server."` to the application *while connected*.
   Fix: give `init()` private `readLine()`/`writeRaw()` helpers that throw instead of
   self-healing, so `processException` is the only reconnect loop. Add `$total = 0;` in the
   `catch` at [Connection.php:188](src/Connection.php#L188) regardless.
5. **Queue `group` is dropped on resubscribe.** [Client.php:218-221](src/Client.php#L218-L221)
   stores only `name` + `sid`, and [Connection.php:356-360](src/Connection.php#L356-L360)
   replays without `group`, so after a reconnect a queue subscriber silently becomes a
   plain subscriber and every group member gets every message. Pre-existing, but this change
   turns failover from exceptional into routine. Two-line fix.

### Configuration — [src/Configuration.php](src/Configuration.php)

New promoted properties, all with backward-compatible defaults:

| Option | Default | nats.go equivalent |
| --- | --- | --- |
| `servers` | `[]` | `Options.Servers` |
| `noRandomize` | `false` | `Options.NoRandomize` |
| `ignoreDiscoveredServers` | `false` | `Options.IgnoreDiscoveredServers` |
| `reconnectWait` | `0.2` | `Options.ReconnectWait` |
| `reconnectJitter` | `0.1` | `Options.ReconnectJitter` |
| `discoveredServersHandler` | `null` | `Options.DiscoveredServersCB` |
| `lameDuckModeHandler` | `null` | `Options.LameDuckModeHandler` |

Handlers are typed `?Closure`, not `?callable` — `callable` is not a legal property type in
PHP. The legacy-array path ([Configuration.php:51-62](src/Configuration.php#L51-L62))
assigns via `property_exists`, so closures flow through unchanged.

`servers` accepts `host:port`, `nats://host:port`, `tls://host:port`, and
`nats://user:pass@host:port`. **When `servers` is non-empty, `host`/`port` are ignored** —
what README.md:442 already claims.

`getOptions()` gains **`'protocol' => 1`**.

**`reconnectWait`/`reconnectJitter` are new rather than reusing `Configuration::delay()`.**
`delay()` is the *message-polling* knob ([Connection.php:140](src/Connection.php#L140),
[:308](src/Connection.php#L308)), wanted at sub-millisecond scale; reconnect backoff wants
seconds. Reusing it would hammer a downed cluster at ~1000 attempts/sec on the default
`delay = 0.001` with `maxReconnectAttempts = -1`. It is also subtly broken for this
purpose: exponential mode computes `intval(0.001 * 1000) ** $iteration` = `1 ** n`, a flat
1 ms forever, and `delay($iteration - 1)` at
[Connection.php:338](src/Connection.php#L338) makes the first retry sleep zero in linear
mode (which `FunctionalTestCase` uses).

### New: `src/Connection/Server.php`

One pool entry, mirroring nats.go's `Server` struct:

- readonly: `host`, `port`, `secure`, `isImplicit`, `tlsName`, `user`, `pass`, `token`
- mutable: `reconnects`, `didConnect`, `lastError`, `draining`
- `getAddress(): string` — the dedup key. Must emit **exactly the form the server
  advertises**, i.e. `net.JoinHostPort` shape (`[::1]:4222` for IPv6), or dedup silently
  fails. **Never include userinfo** — that is the nats.py legacy bug.
- `getDsn(): string` — `tcp://host:port`, bracketing bare IPv6. This is required, not
  cosmetic: `stream_socket_client("::1:4299")` fails with `getaddrinfo for  failed` and
  errno **0**, which at [Connection.php:232](src/Connection.php#L232) degrades into
  `Exception("Connection error", 0)`. The existing DSN at
  [:227](src/Connection.php#L227) is already broken for `host: '::1'`, so this is a free win.

Parse the host/port split with `strrpos($addr, ':')`, never `explode(':')` — IPv6 breaks
the latter. `hostIsIp()` is `filter_var(trim($h, '[]'), FILTER_VALIDATE_IP)`.

### New: `src/Connection/ServerPool.php`

Holds `Server[]`, a never-pruned set of ever-seen addresses (nats.go's `nc.urls`), and the
`current` entry.

- **Setup**: seed from `servers` if non-empty, else the single `host:port`; shuffle from
  offset 0 unless `noRandomize`; element 0 becomes current. **Built lazily on first
  `init()`, not in `Connection::__construct`** — otherwise post-construction config
  mutation (which existing tests do, e.g. [ConnectionTest.php:203](tests/Unit/ConnectionTest.php#L203))
  is silently ignored, and `testLazyConnection` ([ClientTest.php:106-110](tests/Functional/ClientTest.php#L106-L110))
  breaks, since it constructs with `port: -1` and asserts nothing happens. No port
  validation at construction for the same reason.
- **`next()`** — nats.go `selectNextServer`. Use `array_splice()` exclusively, never
  `unset()`: after `unset` the array is no longer a list and `next()`'s "index 0 is
  current" assumption breaks, which would surface as an undefined index deep inside a
  reconnect.
- **`processInfo(Info $info, bool $initial): PoolUpdate`** — the algorithm above, verbatim.
  Early-return on empty/absent `connect_urls` *or* `ignoreDiscoveredServers` (which must
  suppress removals as well as additions, as nats.go does). Returns added/removed plus
  `hasNew`.
- `markConnected()` / `markFailed()` for the per-server counters.
- `getServers()` / `getDiscoveredServers()` returning `scheme://host:port` strings with
  userinfo stripped.
- **`ws_connect_urls` ([Info.php:22](src/Message/Info.php#L22)) is explicitly ignored** —
  it advertises websocket endpoints, unusable here. State the exclusion so nobody
  "helpfully" adds it as a fallback.

Read `Info` fields with `??`, never directly: those properties have no defaults and are
*uninitialized* rather than null when omitted, so `$info->ldm` throws while
`$info->ldm ?? false` is safe (verified). Adding `= null` defaults is not an option — it
would change what `render()` serializes and break
[FactoryTest.php:19-39](tests/Unit/Message/FactoryTest.php#L19-L39).

The "is this the current server" test is `===` on the `Server` object. PHP arrays hold
object *references*, so splice/shuffle/re-append preserve identity, making it exactly
equivalent to nats.go's pointer equality. The rule that keeps it safe: **pool entries are
never cloned** — hence `getServers()` returns strings, not objects. On eviction, reassign
or null `current` in the same operation so it can never point outside the pool.

### `src/Connection.php`

- Hold a `ServerPool`; add `bool $initialConnect` (nats.go's `nc.initc`) and
  `bool $tlsEnabled`.
- **`init()`**: DSN from `pool->current()`; record current in the pool **before** the INFO
  read, or discovered servers inherit no scheme and no credentials; pass `tlsName` as the
  TLS peer name; apply per-server credentials to the `Connect` message.
- **Complete the handshake before `markConnected()`.** NATS rejects auth *asynchronously*:
  the server accepts the CONNECT bytes, then emits `-ERR 'Authorization Violation'` and
  closes. Marking success on the write would reset `reconnects` to 0 on a server that will
  always reject, so **`maxReconnectAttempts` never exhausts and eviction never happens** —
  defeating the whole point of per-server budgets. It would also surface the `-ERR` as an
  uncaught `LogicException` from [Factory.php:28-31](src/Message/Factory.php#L28-L31)
  inside some later unrelated `getMessage()`. So: write CONNECT **then PING**, read to
  `PONG`, and only then `markConnected()`. One extra RTT, matching every other client.
  A handshake ping/pong leaves `pongAt > pingAt`, so `ping()`
  ([:155-161](src/Connection.php#L155-L161)) still returns true. Add nats.go's auth-error
  latch: two consecutive auth failures from one server ⇒ evict rather than retry.
- **Reset per-connection state at the top of `init()`**: `$tlsEnabled` (otherwise the
  second server's TLS handshake is skipped and a **plaintext CONNECT goes to a TLS port**,
  since `init()` builds a fresh `$context`), plus `$activityAt`, `$pingAt`, `$pongAt`,
  `$prolongateTill`. Stale values make all three conditions at
  [:144-149](src/Connection.php#L144-L149) true on the fresh socket ⇒ immediate
  `processException` ⇒ reconnect storm. `Connection::close()`
  ([:373-379](src/Connection.php#L373-L379)) must reset the same fields.
- **`getMessage()` Info branch** ([:123-130](src/Connection.php#L123-L130)): guard the TLS
  upgrade with `$tlsEnabled` — once `protocol => 1` makes async INFO real, an async INFO
  carrying `tls_required` would otherwise re-run `stream_socket_enable_crypto` on an
  already-encrypted socket. Then refresh `$this->infoMessage` and call `processInfo()`.
  **Merge into the existing `Info` rather than replacing it wholesale**, so an async INFO
  that omits `tls_*` cannot leave a previously-initialized property uninitialized —
  [ClientTest.php:136-137](tests/Functional/ClientTest.php#L136-L137) reads those directly,
  and direct access to an uninitialized typed property throws `Error`.
- **Handler dispatch is the sharpest remaining hazard.** `processInfo` fires from inside
  `getMessage()`, possibly inside `init()`, possibly inside `processException()`. So:
  (a) fire handlers only for *async* INFO, gated on `$initialConnect`, never for the
  handshake INFO — otherwise a handler calling `publish()` writes on a socket whose CONNECT
  has not been sent; (b) make `forceReconnect()` **deferred** — set a flag, act at the top
  of the next `getMessage()`/`sendMessage()` — since the documented LDM idiom is to call it
  *from* `lameDuckModeHandler`, which would otherwise recurse
  `processException → init → getMessage → processInfo → handler → forceReconnect`;
  (c) wrap handler invocation in try/catch-and-log so a throwing app handler cannot abort a
  reconnect mid-flight. nats.go dispatches these on a separate goroutine precisely to avoid
  all three.
- **`processException()`**: walk `pool->next()`; sleep `reconnectWait + jitter` once per
  full pass. The pass counter must re-read `count()` each iteration — `processInfo` can grow
  or shrink the pool mid-pass, so neither a cached count nor a remembered pass-start
  `Server` (it may be evicted) is safe. Also fix the shadowed `$e` at
  [:349](src/Connection.php#L349) so the original disconnect cause surfaces when the pool
  drains, and add the missing `continue` after `processException` at
  [:70](src/Connection.php#L70).
- **`init()`'s guard is truthiness**, `if ($this->socket)` at
  [:222](src/Connection.php#L222), while [:68](src/Connection.php#L68) uses `is_resource()`.
  A closed-but-non-null resource is truthy, so `init()` returns and the next `fwrite` fails.
  Make them agree.
- **Classify `-ERR`.** nats.go treats `Stale Connection`, `Server Shutdown` and
  `Slow Consumer` as reconnect triggers; today all `-ERR`s become a `LogicException` from
  `Factory::create` thrown out of [:106](src/Connection.php#L106), uncaught, to the app — so
  an LDM node closing with an error line hands the app an exception instead of failing over.
  This repo has no custom exception hierarchy, so either match on the message (works, zero
  BC risk) or introduce a `Message\Error` type (cleaner, but a BC break for anyone catching
  `LogicException` around `process()`). **Recommend message-matching now**, and note the
  alternative.
- **`forceReconnect()`** must `fclose` **and** null the socket.

### Two deliberate deviations from nats.go

1. **Credential precedence is additive, not mutually exclusive.** In nats.go, URL userinfo
   means `Options.User/Password/Token/Nkey` are not consulted at all. Copying that would
   mean a `servers` entry with `user:pass@` silently disables `Configuration::$nkey` — but
   this repo builds its `Authenticator` once in the `Connection` constructor
   ([:42](src/Connection.php#L42)) and signs the nonce in `init()`, so that would break
   nkey auth unexpectedly. Instead: per-server userinfo overrides only
   `user`/`pass`/`auth_token` on the `Connect` message; nkey/JWT signing is untouched and
   already correct across failover, since it re-signs each server's nonce. Keep nats.go's
   "username with no password means token" rule, and slot the override into the existing
   exclusive `elseif` chain at [Configuration.php:75-82](src/Configuration.php#L75-L82) —
   emitting both `user` and `auth_token` earns an `Authorization Violation`.
2. **`servers` and `host`/`port` are mutually exclusive** rather than merged. nats.go can
   append `Opts.Url` and swap it to the front because its `Url` defaults to `""`. Here
   `host`/`port` default to `localhost:4222`, so "user set it" is indistinguishable from
   "untouched" and merging would inject `localhost:4222` into every clustered pool.

### Control-line truncation — [Connection.php:103](src/Connection.php#L103)

`stream_get_line($this->socket, 1024, "\r\n")` needs more than a bigger constant. Measured
behaviour: an over-length line returns exactly `$length` bytes **without consuming the
delimiter**, and the remainder comes back on subsequent calls as though it were new
protocol lines. An exactly-1024-byte line also returns 1024 and then `''`. So an oversized
INFO gives (a) a first chunk cut mid-JSON — `json_decode` returns `null` and
[Prototype.php:26-28](src/Message/Prototype.php#L26-L28) early-returns leaving *every*
property uninitialized, while `instanceof Info` at [:252](src/Connection.php#L252) still
passes, i.e. a "successful" handshake against a blank `Info` — and then (b) leftover chunks
fed to `Factory::create`, throwing `RuntimeException`/`LogicException` from an unrelated
call site. Under `reconnect => true` those reach `processException`, so the client
reconnects, re-reads the same INFO, and loops.

Three parts, all needed:

1. A private `readLine()` looping `stream_get_line($socket, self::CHUNK, "\r\n")` while the
   return is exactly `CHUNK` bytes, concatenating. Termination is exact: a *short* read
   means the delimiter was found and consumed; a *full-length* read means truncation or
   exact fit, and exact fit yields `''` next. Distinguish `false` (timeout) from `''`
   (exact-fit terminator) with `===` — [:105](src/Connection.php#L105)'s `if ($line)`
   currently conflates them. Keep 1024 as the *chunk* unit; the loop, not the size, is what
   makes this independent of cluster size.
2. A hard overall cap that **throws** instead of truncating.
3. Make `Prototype` loud: throw when a non-empty `Payload`'s `getValues()` is `null`. This
   is the actual silence bug. Safe — `Msg::create` passes an array and bypasses the
   `Payload` branch; the only `Payload` callers are the five JSON/space-delimited cases in
   `Factory.php`.

Scale check, so this is neither oversold nor dismissed: 30 nodes advertising short IPs is
~918 bytes and fine. Long hostnames break it — 15 nodes on Kubernetes DNS names
(`nats-N.nats-headless.<ns>.svc.cluster.local:4222`) is ~1200 bytes. A small CI cluster will
never reproduce it, which is why the regression test must synthesize an oversized INFO.

### Docs — [README.md:429-451](README.md#L429-L451)

Document the new options and add a cluster example. Reword the `maxReconnectAttempts` row
for per-server semantics. Note that inheriting userinfo onto **implicit** servers sends
credentials to addresses the *server* chose, and that `ignoreDiscoveredServers` is the
escape hatch.

## Implementation order

Each step compiles and keeps the suite green on its own.

1. **Prerequisite fixes** (the five above + truncation). No new features; own commits.
2. **`Server` + `ServerPool` with unit tests, wired to nothing.** Pure, no I/O.
3. **Configuration options + README.** Still inert.
4. **Wire the pool into `Connection`** — lazy pool build, DSN from the pool, state reset,
   non-reentrant handshake with PING/PONG, `processInfo()` hook. Failover works from here.
5. **Emit `protocol => 1`.** The switch that makes async INFO arrive — deliberately after
   step 4, so the discovery path exists before the first async INFO shows up.
6. **LDM handling + deferred `forceReconnect()`.**
7. **Cluster services in docker-compose + functional tests.**

## Verification

**Unit** — `tests/Unit/Connection/ServerPoolTest.php`, extending the pure `Tests\TestCase`.
Note `tests/Unit/ConnectionTest.php` and `tests/Unit/ClientTest.php` extend
`FunctionalTestCase`, whose `tearDown` connects to a live server and deletes all streams
even for tests that never used a client — do not follow those; follow
[FactoryTest.php:15](tests/Unit/Message/FactoryTest.php#L15).

Design `ServerPool` as a pure `Info` → pool transformation **with an injectable shuffler**,
so order assertions are deterministic. Cases: seeding from `servers` vs `host`/`port`;
`noRandomize`; adds implicit / dedups against explicit / removes vanished implicit / never
removes explicit / never removes current; empty `connect_urls` causes no removals; `hasNew`
only for never-seen addresses (a flapping server re-enters without re-firing);
`initial = true` suppresses handlers; `ignoreDiscoveredServers` suppresses additions *and*
removals; per-server budget, eviction, drained-pool throw; `tlsName` set for
hostname-current + IP-advertised, not for IP-current, explicit override wins; userinfo
inherited but never in `getAddress()`; IPv6 in `net.JoinHostPort` form.

**Functional, fake server.** The socket-pair trick from
[ConnectionFdLeakTest.php:86-87](tests/Functional/ConnectionFdLeakTest.php#L86-L87) works
only because it reflects a pre-made pair *over an already-initialized socket* — `init()`
never runs against it. So it can test the **async** paths and nothing else: feed
`INFO {...,"ldm":true}`, assert the handler fired, the entry is draining, and the
connection is still open; feed `connect_urls`, assert pool contents and the discovered
handler; feed an oversized INFO as the truncation regression. That is the right home for
LDM and discovery tests, and it is platform-portable — only the FD *counting* in that file
is Linux-only.

For handshake and failover you need a real connect target. `pcntl` is not in the CI
`php_extensions` list, so use **`proc_open('php -r …')` as a scripted fake server**
(portable, no extra extensions): listen, send INFO with `connect_urls`, expect CONNECT,
close.

**Functional, real cluster** — three clustered services in
[docker/docker-compose.yml](docker/docker-compose.yml), leaving the existing four
untouched. Two things this will get wrong by default:

- A compose cluster advertises **container-internal IPs** (`172.18.0.3:4222`), unreachable
  from the CI host where PHP runs, so every discovered server fails to connect. Set
  `--client_advertise 127.0.0.1:<published-port>` per node.
- [tests.yml](.github/workflows/tests.yml) runs `docker compose up -d` then `phpunit` with
  no readiness wait. A 3-node mesh takes longer to form than a single node and
  `connect_urls` is complete only afterwards, so pool-size assertions flake. Poll
  `http://localhost:8222/varz` for route count, or retry-with-timeout inside the test.

Then: connect to one node and assert `getDiscoveredServers()` picks up the other two;
deterministic failover with no docker manipulation via `servers` = two dead ports followed
by one live one; LDM end-to-end via `docker compose kill -s SIGUSR2 <node>`, skipped when
docker is unavailable. Use `delay: 0.001, delayMode: constant` as
[ConnectionFdLeakTest.php:69-70](tests/Functional/ConnectionFdLeakTest.php#L69-L70) does —
`FunctionalTestCase`'s `delay: 0.05, LINEAR` will burn seconds across a 3-node pool.

**Gates** — `composer test`, `composer cs-verify`, `composer phan`. Phan runs at
`SEVERITY_LOW` on `target_php_version 8.1`, so no 8.2+ syntax. phpstan and rector are in
`require-dev` but have no config and no CI job, so they are not gates. New files must pass
`editorconfig-checker` (LF, final newline, 4-space PHP) — a separate CI job from
php-cs-fixer and an easy red build on a new directory.

## Backward-compatibility watch list

| Existing behavior | Impact | Verdict |
| --- | --- | --- |
| `maxReconnectAttempts` documented as "per disconnect" ([README.md:437](README.md#L437)) | Becomes per-server; with N servers total attempts rise to ~N×. Default `-1` means single-server users see no change, and `ConnectionFdLeakTest`'s `5` still terminates because each reconnect succeeds and resets the counter — which makes it a useful canary for the `markConnected` placement above. | Accepted (user's decision); update README |
| [FactoryTest.php:19-39](tests/Unit/Message/FactoryTest.php#L19-L39) exact `get_object_vars` + render round-trip | Breaks only if `Info` properties gain `= null`. Design forbids that. | Fine |
| [StreamTest.php:155-159](tests/Functional/StreamTest.php#L155-L159) asserts `assertSame` object identity of the info message | Refreshing `$infoMessage` on async INFO makes this a latent flake on a clustered server (single-node CI sends no topology updates, so it stays green). | Change the test — compare `server_id` — and add a positive test that async INFO *does* update it |
| [ClientTest.php:136-137](tests/Functional/ClientTest.php#L136-L137) reads `tls_required`/`tls_verify` directly | A leaner async INFO replacing a complete one would make direct access throw `Error`. | Addressed by merging rather than replacing `Info` |
| `testLazyConnection` (`port: -1`) | Breaks if the pool is built eagerly or validates the port. | Addressed by lazy pool construction |
| `testInvalidConnection` regex `/^Connection refused$\|…/` | Verified: `stream_socket_client` returns the same errno 61 / `"Connection refused"` for `localhost:-1` and `tcp://localhost:-1`, so the explicit scheme in `getDsn()` is safe. | Fine |
| `protocol => 1` makes the server push async INFO | `getMessage()` already returns `Info` to callers ([:129](src/Connection.php#L129)) and `Client::process()` passes it through ([Client.php:198-200](src/Client.php#L198-L200)), so app loops will now occasionally see one where they never did. `Client::dispatch()` already tolerates it. | Document |
| `-ERR` currently always a `LogicException` | Reclassifying transient errors into `processException` changes behavior for apps catching `LogicException` around `process()`. | Flag explicitly, don't change silently |
| In-flight requests are lost across failover | `Client::$handlers[$replyTo]` entries are orphaned (the reply went to the old server), so `dispatch()` spins to its threshold and throws `"Processing timeout"`. Same for in-flight `$JS.API.CONSUMER.MSG.NEXT` pulls. | Not fixable here; document |
| `getAddress()` normalization | `localhost:4222` will not dedup against an advertised `127.0.0.1:4222`, so a 3-node cluster can yield a 4-entry pool. nats.go has the same wart. | Accepted; don't be surprised |
