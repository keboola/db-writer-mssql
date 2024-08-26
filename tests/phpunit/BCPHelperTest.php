<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\DbWriter\Writer\BCPHelper;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;

class BCPHelperTest extends TestCase
{
    /** @dataProvider columnsProvider */
    public function testConvertColumns(array $provided, array $expected): void
    {
        $items = array_map(fn($v) => ItemConfig::fromArray($v), $provided);
        $bcpHelper = new BCPHelper($this->createMock(MSSQLConnection::class));
        $this->assertSameItemObjects($expected, $bcpHelper->convertColumns($items));
    }

    public function columnsProvider(): array
    {
        return [
            'ignored columns' => [
                'provided' => [
                    [
                        'type' => 'ignore',
                        'size' => '1',
                        'dbName' => 'foo',
                        'name' => 'foo',
                    ],
                    [
                        'type' => 'ignore',
                        'size' => '10',
                        'dbName' => 'boo',
                        'name' => 'boo',
                    ],
                ],
                'expected' => [],
            ],
            'string columns' => [
                'provided' => [
                    [
                        'type' => 'char',
                        'size' => '1',
                        'dbName' => 'foo',
                        'name' => 'foo',
                    ],
                    [
                        'type' => 'nchar',
                        'size' => '10',
                        'dbName' => 'boo',
                        'name' => 'boo',
                    ],
                    [
                        'type' => 'varchar',
                        'size' => '100',
                        'dbName' => 'fooBoo',
                        'name' => 'fooBoo',
                    ],
                    [
                        'type' => 'nvarchar',
                        'size' => '1000',
                        'dbName' => 'booFoo',
                        'name' => 'booFoo',
                    ],
                    [
                        'type' => 'binary',
                        'size' => '10000',
                        'dbName' => 'kochba',
                        'name' => 'kochba',
                    ],
                    [
                        'type' => 'varbinary',
                        'size' => '100000',
                        'dbName' => 'barKochba',
                        'name' => 'barKochba',
                    ],
                ],
                'expected' => [
                    ItemConfig::fromArray([
                        'name' => 'foo',
                        'dbName' => 'foo',
                        'type' => 'NVARCHAR',
                        'size' => '1',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'boo',
                        'dbName' => 'boo',
                        'type' => 'NVARCHAR',
                        'size' => '10',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'fooBoo',
                        'dbName' => 'fooBoo',
                        'type' => 'NVARCHAR',
                        'size' => '100',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'booFoo',
                        'dbName' => 'booFoo',
                        'type' => 'NVARCHAR',
                        'size' => '1000',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'kochba',
                        'dbName' => 'kochba',
                        'type' => 'NVARCHAR',
                        'size' => 'MAX',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'barKochba',
                        'dbName' => 'barKochba',
                        'type' => 'NVARCHAR',
                        'size' => 'MAX',
                        'nullable' => true,
                    ]),
                ],
            ],
            'other columns' => [
                'provided' => [
                    [
                        'type' => 'date',
                        'size' => '10',
                        'dbName' => 'foo',
                        'name' => 'foo',
                    ],
                    [
                        'type' => 'datetime',
                        'size' => '100',
                        'dbName' => 'boo',
                        'name' => 'boo',
                    ],
                    [
                        'type' => 'int',
                        'size' => '1000',
                        'dbName' => 'bar',
                        'name' => 'bar',
                    ],
                    [
                        'type' => 'money',
                        'size' => '100',
                        'dbName' => 'fooBar',
                        'name' => 'fooBar',
                    ],
                    [
                        'type' => 'time',
                        'size' => '10',
                        'dbName' => 'far',
                        'name' => 'far',
                    ],
                    [
                        'type' => 'timestamp',
                        'size' => '100',
                        'dbName' => 'barFoo',
                        'name' => 'barFoo',
                    ],
                ],
                'expected' => [
                    ItemConfig::fromArray([
                        'name' => 'foo',
                        'dbName' => 'foo',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'boo',
                        'dbName' => 'boo',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'bar',
                        'dbName' => 'bar',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'fooBar',
                        'dbName' => 'fooBar',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'far',
                        'dbName' => 'far',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'barFoo',
                        'dbName' => 'barFoo',
                        'type' => 'NVARCHAR',
                        'size' => '255',
                        'nullable' => true,
                    ]),
                ],
            ],
            'text columns' => [
                'provided' => [
                    [
                        'type' => 'text',
                        'size' => '1000',
                        'dbName' => 'foo',
                        'name' => 'foo',
                    ],
                    [
                        'type' => 'ntext',
                        'size' => '100000',
                        'dbName' => 'bor',
                        'name' => 'bor',
                    ],
                    [
                        'type' => 'image',
                        'size' => '1000000',
                        'dbName' => 'bar',
                        'name' => 'bar',
                    ],
                ],
                'expected' => [
                    ItemConfig::fromArray([
                        'name' => 'foo',
                        'dbName' => 'foo',
                        'type' => 'NVARCHAR',
                        'size' => 'MAX',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'bor',
                        'dbName' => 'bor',
                        'type' => 'NVARCHAR',
                        'size' => 'MAX',
                        'nullable' => true,
                    ]),
                    ItemConfig::fromArray([
                        'name' => 'bar',
                        'dbName' => 'bar',
                        'type' => 'NVARCHAR',
                        'size' => 'MAX',
                        'nullable' => true,
                    ]),
                ],
            ],
        ];
    }

    private function assertSameItemObjects(array $expected, array $convertColumns): void
    {
        $this->assertCount(count($expected), $convertColumns);
        foreach ($expected as $key => $item) {
            $convertColumn = $convertColumns[$key];
            $this->assertSame($item->getName(), $convertColumn->getName());
            $this->assertSame($item->getDbName(), $convertColumn->getDbName());
            $this->assertSame($item->getType(), $convertColumn->getType());
            $this->assertSame($item->getSize(), $convertColumn->getSize());
            if ($item->hasDefault()) {
                $this->assertSame($item->getDefault(), $convertColumn->getDefault());
            }
        }
    }
}
