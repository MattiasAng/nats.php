<?php

declare(strict_types=1);

namespace Basis\Nats\Connection;

use Basis\Nats\Configuration;
use Basis\Nats\Message\Info;
use Closure;
use InvalidArgumentException;
use RuntimeException;
use Throwable;

/**
 * The set of servers the client may connect to: the ones configured explicitly plus
 * the ones the cluster advertises through connect_urls.
 *
 * Pool entries are never cloned, so identity comparison is enough to recognise the
 * current server wherever it has been moved to by rotation or shuffling.
 */
class ServerPool
{
    /** @var Server[] */
    private array $servers = [];

    /**
     * Every address ever admitted to the pool. Deliberately never pruned: a server
     * that drops out of connect_urls and comes back is not a new discovery, so it
     * must not notify again.
     *
     * @var array<string, true>
     */
    private array $known = [];

    private ?Server $current = null;

    private Closure $shuffle;

    /**
     * @param Closure|null $shuffle seam for deterministic ordering in tests
     */
    public function __construct(
        private Configuration $config,
        ?Closure $shuffle = null,
    ) {
        $this->shuffle = $shuffle ?? static function (array $servers): array {
            shuffle($servers);
            return $servers;
        };

        if ($config->servers !== []) {
            foreach ($config->servers as $url) {
                $this->append(Server::fromUrl((string) $url));
            }
        } else {
            // Built straight from host and port rather than parsed, so that values
            // like a negative port still reach the socket and fail there.
            $this->append(new Server(host: $config->host, port: $config->port));
        }

        if (!$config->noRandomize) {
            $this->shufflePool(0);
        }

        $this->current = $this->servers[0] ?? null;
    }

    public function current(): ?Server
    {
        return $this->current;
    }

    public function count(): int
    {
        return count($this->servers);
    }

    /**
     * Moves to the next server, rotating the current one to the back of the pool.
     *
     * A server that has used up its reconnect budget is dropped instead, so a node
     * that is permanently gone stops consuming attempts.
     */
    public function next(): Server
    {
        $index = $this->indexOfCurrent();

        if ($index === null) {
            throw new RuntimeException('No servers available');
        }

        $server = $this->servers[$index];
        array_splice($this->servers, $index, 1);

        $budget = $this->config->maxReconnectAttempts;
        if ($budget < 0 || $server->reconnects < $budget) {
            $this->servers[] = $server;
        }

        if ($this->servers === []) {
            $this->current = null;
            throw new RuntimeException('No servers available');
        }

        return $this->current = $this->servers[0];
    }

    public function markConnected(Server $server): void
    {
        $server->didConnect = true;
        $server->reconnects = 0;
        $server->lastError = null;
        $server->draining = false;
        $server->authenticationFailed = false;
    }

    /**
     * Drops a server for good, for failures that retrying cannot resolve.
     */
    public function evict(Server $server): void
    {
        foreach ($this->servers as $index => $candidate) {
            if ($candidate === $server) {
                array_splice($this->servers, $index, 1);
                break;
            }
        }

        if ($this->current === $server) {
            $this->current = $this->servers[0] ?? null;
        }
    }

    public function markFailed(Server $server, Throwable $error): void
    {
        $server->reconnects++;
        $server->lastError = $error;
    }

    /**
     * Applies the cluster topology from an INFO message.
     *
     * @param bool $initial true while the connection is still being established,
     *                      when the pool is populated silently
     */
    public function processInfo(Info $info, bool $initial = false): PoolUpdate
    {
        $advertised = $info->connect_urls ?? [];

        // An absent or empty list means the server is not advertising, not that the
        // cluster is gone, so nothing may be removed on its word.
        if ($advertised === [] || $this->config->ignoreDiscoveredServers) {
            return new PoolUpdate();
        }

        $unmatched = [];
        foreach ($advertised as $address) {
            $unmatched[(string) $address] = true;
        }

        $removed = [];
        $retained = [];

        foreach ($this->servers as $server) {
            $address = $server->getAddress();
            $stillAdvertised = isset($unmatched[$address]);

            // Consumed for every entry, explicit ones included, so that a configured
            // server which is also advertised is not added again as a discovered one.
            unset($unmatched[$address]);

            // Explicitly configured servers stay, and so does the one we are talking
            // to even when it was itself discovered.
            if ($stillAdvertised || !$server->isImplicit || $server === $this->current) {
                $retained[] = $server;
                continue;
            }

            $removed[] = $address;
        }

        $this->servers = $retained;

        // Only worth remembering a name when the current address actually is one.
        $inheritTlsName = $this->current !== null && !$this->current->hostIsIpAddress();

        $added = [];
        $hasNew = false;

        foreach (array_keys($unmatched) as $address) {
            $server = $this->discover($address, $inheritTlsName);

            if ($server === null) {
                continue;
            }

            if (!isset($this->known[$address])) {
                $hasNew = true;
            }

            $this->servers[] = $server;
            $this->known[$address] = true;
            $added[] = $address;
        }

        if ($hasNew && !$this->config->noRandomize) {
            // Advertised servers arrive in no particular order, so only the entry we
            // are connected to needs to keep its place.
            $this->shufflePool(1);
        }

        return new PoolUpdate($added, $removed, $hasNew);
    }

    /** @return string[] every server in the pool, as urls without credentials */
    public function getServers(): array
    {
        return array_map(fn (Server $server) => $server->getUrl(), $this->servers);
    }

    /** @return string[] only the servers learned from connect_urls */
    public function getDiscoveredServers(): array
    {
        $discovered = array_filter($this->servers, fn (Server $server) => $server->isImplicit);

        return array_values(array_map(fn (Server $server) => $server->getUrl(), $discovered));
    }

    /**
     * Builds an entry for an advertised address, inheriting the scheme and the
     * credentials of the server that told us about it, since connect_urls carries
     * neither. A malformed address is skipped rather than fatal, matching the go
     * client.
     */
    private function discover(string $address, bool $inheritTlsName): ?Server
    {
        $current = $this->current;

        try {
            $server = Server::fromUrl($address, isImplicit: true);
        } catch (InvalidArgumentException) {
            return null;
        }

        return new Server(
            host: $server->host,
            port: $server->port,
            secure: $current?->secure ?? false,
            isImplicit: true,
            // A cluster advertises addresses, so a certificate issued for the name we
            // were configured with cannot be verified against them.
            tlsName: $inheritTlsName && $server->hostIsIpAddress() ? $current?->host : null,
            user: $current?->user,
            pass: $current?->pass,
            token: $current?->token,
        );
    }

    private function append(Server $server): void
    {
        $this->servers[] = $server;
        $this->known[$server->getAddress()] = true;
    }

    private function indexOfCurrent(): ?int
    {
        foreach ($this->servers as $index => $server) {
            if ($server === $this->current) {
                return $index;
            }
        }

        return $this->servers === [] ? null : 0;
    }

    private function shufflePool(int $offset): void
    {
        if (count($this->servers) <= $offset + 1) {
            return;
        }

        $head = array_slice($this->servers, 0, $offset);
        $tail = array_slice($this->servers, $offset);

        $this->servers = array_merge($head, ($this->shuffle)($tail));
    }
}
