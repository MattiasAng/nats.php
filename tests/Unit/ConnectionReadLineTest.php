<?php

declare(strict_types=1);

namespace Tests\Unit;

use Basis\Nats\Client;
use Basis\Nats\Configuration;
use Basis\Nats\Connection;
use Basis\Nats\Message\Info;
use InvalidArgumentException;
use ReflectionProperty;
use Tests\TestCase;

/**
 * A protocol line longer than a single read chunk used to be cut in half:
 * stream_get_line() returns the chunk without consuming the delimiter and hands the
 * remainder back on the next call, so a large clustered INFO decoded to nothing and
 * its leftovers reached Factory as unparsable garbage.
 */
class ConnectionReadLineTest extends TestCase
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

    public function testInfoLongerThanOneChunkIsReadWhole(): void
    {
        // 40 nodes advertised with kubernetes style dns names: well past one chunk.
        $urls = [];
        for ($i = 0; $i < 40; $i++) {
            $urls[] = "nats-$i.nats-headless.production.svc.cluster.local:4222";
        }

        $info = [
            'server_id' => str_repeat('N', 56),
            'server_name' => str_repeat('N', 56),
            'version' => '2.11.4',
            'host' => '0.0.0.0',
            'port' => 4222,
            'headers' => true,
            'max_payload' => 1048576,
            'proto' => 1,
            'connect_urls' => $urls,
        ];

        $line = 'INFO ' . json_encode($info);
        $this->assertGreaterThan(1024, strlen($line), 'the fixture has to exceed one chunk');

        $info = $this->readInfo($line . "\r\n");

        $this->assertSame($urls, $info->connect_urls);
        $this->assertSame(1048576, $info->max_payload);
    }

    public function testLineOfExactlyOneChunkIsReadWhole(): void
    {
        $prefix = 'INFO {"server_name":"';
        $suffix = '","proto":1}';
        $padding = 1024 - strlen($prefix) - strlen($suffix);
        $name = str_repeat('x', $padding);
        $line = $prefix . $name . $suffix;

        $this->assertSame(1024, strlen($line));

        $info = $this->readInfo($line . "\r\n");

        $this->assertSame($name, $info->server_name);
    }

    /**
     * Truncated json used to leave every property uninitialized while the message
     * still satisfied an instanceof check.
     */
    public function testDamagedLineIsRejected(): void
    {
        $connection = $this->connectionReading('INFO {"server_name":"cut-off' . "\r\n");

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid payload for message');

        $connection->getMessage(0);
    }

    /**
     * Feeds a raw protocol line to a connection over a socket pair and returns the
     * INFO state it ended up with. An asynchronous INFO is not handed back to
     * callers, so the merged message is what there is to observe.
     */
    private function readInfo(string $wire): Info
    {
        $connection = $this->connectionReading($wire);

        // A zero timeout consumes whatever is buffered and returns, so nothing has
        // to be caught here and an unexpected failure is not swallowed.
        $connection->getMessage(0);

        return $connection->getInfoMessage();
    }

    private function connectionReading(string $wire): Connection
    {
        [$clientEnd, $serverEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        $this->sockets[] = $clientEnd;
        $this->sockets[] = $serverEnd;

        fwrite($serverEnd, $wire);

        $client = new Client(new Configuration(['reconnect' => false, 'timeout' => 1]));
        $property = new ReflectionProperty(Connection::class, 'socket');
        $property->setValue($client->connection, $clientEnd);

        return $client->connection;
    }
}
