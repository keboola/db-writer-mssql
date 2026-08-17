<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\DbWriter\Configuration\NodeDefinition\MSSQLDbNode;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Config\Definition\Processor;

class MSSQLDbNodeTest extends TestCase
{
    /**
     * @param array<string, mixed> $dbConfig
     * @dataProvider validConfigProvider
     */
    public function testValidConfig(array $dbConfig): void
    {
        $processed = $this->process($dbConfig);

        self::assertSame($dbConfig['database'], $processed['database']);
    }

    /**
     * @return iterable<string, array{array<string, mixed>}>
     */
    public function validConfigProvider(): iterable
    {
        yield 'sql login' => [[
            'host' => 'mssql',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'password',
        ]];

        yield 'service principal' => [[
            'host' => 'server.database.windows.net',
            'database' => 'test',
            'tenantId' => 'tenant-id',
            'clientId' => 'client-id',
            '#clientSecret' => 'client-secret',
        ]];

        yield 'legacy password key' => [[
            'host' => 'mssql',
            'database' => 'test',
            'user' => 'sa',
            'password' => 'password',
        ]];
    }

    /**
     * @param array<string, mixed> $dbConfig
     * @dataProvider invalidConfigProvider
     */
    public function testInvalidConfig(array $dbConfig, string $expectedMessage): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);

        $this->process($dbConfig);
    }

    /**
     * @return iterable<string, array{array<string, mixed>, string}>
     */
    public function invalidConfigProvider(): iterable
    {
        yield 'partial service principal' => [
            [
                'host' => 'server.database.windows.net',
                'database' => 'test',
                'tenantId' => 'tenant-id',
                'clientId' => 'client-id',
            ],
            'For Service Principal authentication all of "tenantId", "clientId" and '
            . '"#clientSecret" must be set.',
        ];

        yield 'no credentials at all' => [
            [
                'host' => 'mssql',
                'database' => 'test',
            ],
            'Either "user" and "#password" (SQL login) or "tenantId", "clientId" and '
            . '"#clientSecret" (Service Principal) must be set.',
        ];

        yield 'user without password' => [
            [
                'host' => 'mssql',
                'database' => 'test',
                'user' => 'sa',
            ],
            'Either "user" and "#password" (SQL login) or "tenantId", "clientId" and '
            . '"#clientSecret" (Service Principal) must be set.',
        ];
    }

    /**
     * @param array<string, mixed> $dbConfig
     * @return array<string, mixed>
     */
    private function process(array $dbConfig): array
    {
        $treeBuilder = new TreeBuilder('parameters');
        /** @var \Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition $root */
        $root = $treeBuilder->getRootNode();
        $root->children()->append(new MSSQLDbNode());

        /** @var array{db: array<string, mixed>} $processed */
        $processed = (new Processor())->process(
            $treeBuilder->buildTree(),
            ['parameters' => ['db' => $dbConfig]],
        );

        return $processed['db'];
    }
}
