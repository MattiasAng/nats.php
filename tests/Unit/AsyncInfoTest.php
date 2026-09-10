<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use ReflectionProperty;
use Tests\TestCase;

/**
 * Cluster topology and lame duck mode arrive as asynchronous INFO messages. A
 * socket pair stands in for the server so both can be delivered exactly.
 */
class AsyncInfoTest extends TestCase
{
    /** @var resource[] */
    private array $sockets = [];

    public function tearDown(): void
    {
        foreach ($this->sockets as $socket) {
            if (is_resource($socket)) {
                fclose($socket);
            }
        }
        $this->sockets = [];
    }

    public function testDiscoveredServersJoinThePool(): void
    {
        $client = $this->client(['servers' => ['a.example.com:4222'], 'noRandomize' => true]);

        $this->deliver($client, ['connect_urls' => ['a.example.com:4222', 'b.example.com:4222']]);

        $this->assertSame(['nats://b.example.com:4222'], $client->getDiscoveredServers());
        $this->assertCount(2, $client->getServers());
    }

    public function testDiscoveryNotifiesOnce(): void
    {
        $calls = 0;
        $client = $this->client([
            'servers' => ['a.example.com:4222'],
            'noRandomize' => true,
            'discoveredServersHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);
        $this->assertSame(1, $calls);

        // The same address again is not a new discovery.
        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);
        $this->assertSame(1, $calls);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222', 'c.example.com:4222']]);
        $this->assertSame(2, $calls);
    }

    /**
     * Lame duck mode is a notification. The go, python and javascript clients all
     * keep the connection and wait for the server to close it, leaving the decision
     * to migrate to the application.
     */
    public function testLameDuckModeNotifiesAndKeepsTheConnection(): void
    {
        $notified = [];
        $client = $this->client([
            'lameDuckModeHandler' => function (Client $client) use (&$notified) {
                $notified[] = $client->connection->getPool()->current()->getAddress();
            },
        ]);

        $this->deliver($client, ['ldm' => true]);

        $this->assertSame(['localhost:4222'], $notified);
        $this->assertTrue($client->connection->getPool()->current()->draining);

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $this->assertTrue(is_resource($socket->getValue($client->connection)));
    }

    public function testLameDuckModeNotifiesOnlyOnTheTransition(): void
    {
        $calls = 0;
        $client = $this->client([
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['ldm' => true]);
        $this->deliver($client, ['ldm' => true]);

        $this->assertSame(1, $calls, 'the server repeats the flag on every later update');
    }

    public function testNoLameDuckNotificationWithoutTheFlag(): void
    {
        $calls = 0;
        $client = $this->client([
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(0, $calls);
    }

    /**
     * A handler runs inline on the read that delivered the update, so it must not be
     * able to take the connection down.
     */
    public function testThrowingHandlerIsContained(): void
    {
        $client = $this->client([
            'lameDuckModeHandler' => function () {
                throw new \RuntimeException('handler exploded');
            },
        ]);

        $this->deliver($client, ['ldm' => true]);

        $this->assertTrue($client->connection->getPool()->current()->draining);
    }

    public function testIgnoreDiscoveredServersKeepsThePoolAsConfigured(): void
    {
        $calls = 0;
        $client = $this->client([
            'servers' => ['a.example.com:4222'],
            'ignoreDiscoveredServers' => true,
            'discoveredServersHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(['nats://a.example.com:4222'], $client->getServers());
        $this->assertSame(0, $calls);
    }

    /**
     * The lame duck flag still gets through when discovery is switched off, since
     * the two are unrelated concerns carried on the same message.
     */
    public function testLameDuckModeStillNotifiesWhenDiscoveryIsIgnored(): void
    {
        $calls = 0;
        $client = $this->client([
            'ignoreDiscoveredServers' => true,
            'lameDuckModeHandler' => function () use (&$calls) {
                $calls++;
            },
        ]);

        $this->deliver($client, ['ldm' => true, 'connect_urls' => ['b.example.com:4222']]);

        $this->assertSame(1, $calls);
    }

    private function client(array $options): Client
    {
        return new Client(new Configuration($options + ['reconnect' => false, 'timeout' => 1]));
    }

    /** Hands an INFO message to the client over a socket pair standing in for the server. */
    private function deliver(Client $client, array $info): void
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $this->sockets[] = $clientEnd;
        $this->sockets[] = $serverEnd;

        fwrite($serverEnd, 'INFO ' . json_encode($info) . "\r\n");

        $socket = new ReflectionProperty(Connection::class, 'socket');
        $socket->setValue($client->connection, $clientEnd);

        $client->connection->getMessage(0);
    }
}
