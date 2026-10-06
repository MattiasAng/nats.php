<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Message\Ping;
use Exception;
use Psr\Log\AbstractLogger;
use Tests\TestCase;
use Tests\Utils\ScriptedServer;

/**
 * Connection behaviour that depends on the exact order of protocol lines, played by
 * a server that follows a script.
 */
class ConnectionScriptedServerTest extends TestCase
{
    private const INFO = 'INFO {"server_id":"X","server_name":"X","version":"2.10.0","host":"127.0.0.1",'
        . '"port":4222,"headers":true,"max_payload":1048576,"proto":1}' . "\r\n";

    /** A handshake that is accepted, followed by one ping from the application. */
    private const ACCEPTING = [
        ['send', self::INFO],
        ['read'], // CONNECT
        ['read'], // PING
        ['send', "PONG\r\n"],
        ['read'], // the application's PING
    ];

    /**
     * A server with something to say about its cluster may say it before answering
     * the PING. That used to fail an otherwise good connection.
     */
    public function testInfoArrivingBeforeThePongDoesNotFailTheHandshake(): void
    {
        $cluster = 'INFO {"server_id":"X","connect_urls":["127.0.0.1:4999"],"proto":1}' . "\r\n";
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', $cluster],
            ['send', "PONG\r\n"],
            ['read'],
        ]);

        $client = $this->client($server->address);
        $client->connection->sendMessage(new Ping());

        $this->assertSame(['nats://127.0.0.1:4999'], $client->connection->getDiscoveredServers());

        $client->connection->close();
        $this->assertCount(3, $server->finish());
    }

    /**
     * Two rejections in a row retire the server. Each attempt is a new connection, so
     * the server plays the rejection twice.
     */
    public function testServerThatRejectsTheCredentialsTwiceIsDropped(): void
    {
        $server = ScriptedServer::start([
            ['send', self::INFO],
            ['read'],
            ['read'],
            ['send', "-ERR 'Authorization Violation'\r\n"],
        ], 2);

        $client = $this->client($server->address);
        $pool = $client->connection->getPool();

        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));
        $this->assertSame([$this->url($server->address)], $client->connection->getServers());
        $this->assertTrue($pool->current()->authenticationFailed);

        $this->assertFailsWith('Authorization Violation', fn () => $client->connection->sendMessage(new Ping()));
        $this->assertSame([], $client->connection->getServers());

        $server->finish();
    }

    public function testForceReconnectIsHonouredWithAutomaticReconnectionOffAndLogsNoError(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 2);
        $logger = $this->recordingLogger();

        $client = $this->client($server->address, ['reconnect' => false]);
        $client->connection->setLogger($logger);

        $client->connection->sendMessage(new Ping());
        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());
        $client->connection->close();

        $connects = array_filter(
            $server->finish(),
            fn (?string $line) => $line !== null && str_starts_with($line, 'CONNECT ')
        );

        $this->assertCount(2, $connects, 'the requested reconnection has to happen');
        $this->assertSame([], $logger->records('error'), 'a requested reconnection is not a failure');
    }

    /**
     * The failure that stops a requested reconnection is the connection error, not
     * something made up to stand for it.
     */
    public function testForceReconnectReportsTheConnectionErrorWhenNoServerAnswers(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 1);

        $client = $this->client($server->address, ['maxReconnectAttempts' => 1, 'timeout' => 0.3]);
        $client->connection->sendMessage(new Ping());

        $client->forceReconnect();

        try {
            $client->connection->sendMessage(new Ping());
            $this->fail('nothing is left to reconnect to');
        } catch (Exception $e) {
            $this->assertSame(Exception::class, $e::class);
            $this->assertNotSame('No servers available', $e->getMessage());
        }

        $server->finish();
    }

    /**
     * There is nothing to move off before the first connection, so asking to is
     * answered by the connection the next write makes anyway, not by a second one.
     */
    public function testForceReconnectBeforeTheFirstConnectionConnectsOnce(): void
    {
        $server = ScriptedServer::start(self::ACCEPTING, 1);

        $client = $this->client($server->address, ['maxReconnectAttempts' => 1, 'timeout' => 0.5]);
        $client->forceReconnect();
        $client->connection->sendMessage(new Ping());
        $client->connection->close();

        $lines = $server->finish();

        $this->assertCount(
            1,
            array_filter($lines, fn (?string $line) => $line !== null && str_starts_with($line, 'CONNECT '))
        );
        $this->assertSame('PING', $lines[2], 'the application ping is the first thing sent after the handshake');
    }

    private function client(string $address, array $options = []): Client
    {
        return new Client(new Configuration($options + [
            'servers' => [$address],
            'timeout' => 1,
            'reconnectWait' => 0.0,
            'reconnectJitter' => 0.0,
        ]));
    }

    private function url(string $address): string
    {
        return 'nats://' . $address;
    }

    private function assertFailsWith(string $message, callable $action): void
    {
        try {
            $action();
        } catch (Exception $e) {
            $this->assertStringContainsStringIgnoringCase($message, $e->getMessage());

            return;
        }

        $this->fail("expected a failure mentioning '$message'");
    }

    private function recordingLogger(): object
    {
        return new class () extends AbstractLogger {
            /** @var array<int, array{string, string}> */
            private array $records = [];

            public function log($level, string|\Stringable $message, array $context = []): void
            {
                $this->records[] = [(string) $level, (string) $message];
            }

            /** @return string[] messages logged at the given level */
            public function records(string $level): array
            {
                return array_column(
                    array_filter($this->records, fn (array $record) => $record[0] === $level),
                    1
                );
            }
        };
    }
}
