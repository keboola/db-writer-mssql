<?php

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\DbWriter\Writer\SQLTransformer;
use PHPUnit\Framework\TestCase;

class SQLTransformerTest extends TestCase
{
    /**
     * @dataProvider columnsProvider
     * @param array $provided
     * @param array $expected
     */
    public function testConvertColumns(array $provided, array $expected): void
    {
        self::assertSame($expected, SQLTransformer::convertColumns($provided));
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
                    ],
                    [
                        'type' => 'ignore',
                        'size' => '10',
                        'dbName' => 'boo',
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
                    ],
                    [
                        'type' => 'nchar',
                        'size' => '10',
                        'dbName' => 'boo',
                    ],
                    [
                        'type' => 'varchar',
                        'size' => '100',
                        'dbName' => 'fooBoo',
                    ],
                    [
                        'type' => 'nvarchar',
                        'size' => '1000',
                        'dbName' => 'booFoo',
                    ],
                    [
                        'type' => 'binary',
                        'size' => '10000',
                        'dbName' => 'kochba',
                    ],
                    [
                        'type' => 'varbinary',
                        'size' => '100000',
                        'dbName' => 'barKochba',
                    ],
                ],
                'expected' => [
                    '[foo] NVARCHAR (1) NULL',
                    '[boo] NVARCHAR (10) NULL',
                    '[fooBoo] NVARCHAR (100) NULL',
                    '[booFoo] NVARCHAR (1000) NULL',
                    '[kochba] NVARCHAR (10000) NULL',
                    '[barKochba] NVARCHAR (100000) NULL',
                ],
            ],
            'other columns' => [
                'provided' => [
                    [
                        'type' => 'date',
                        'size' => '10',
                        'dbName' => 'foo',
                    ],
                    [
                        'type' => 'datetime',
                        'size' => '100',
                        'dbName' => 'boo',
                    ],
                    [
                        'type' => 'int',
                        'size' => '1000',
                        'dbName' => 'bar',
                    ],
                    [
                        'type' => 'money',
                        'size' => '100',
                        'dbName' => 'fooBar',
                    ],
                    [
                        'type' => 'time',
                        'size' => '10',
                        'dbName' => 'far',
                    ],
                    [
                        'type' => 'timestamp',
                        'size' => '100',
                        'dbName' => 'barFoo',
                    ],
                ],
                'expected' => [
                    '[foo] NVARCHAR (255) NULL',
                    '[boo] NVARCHAR (255) NULL',
                    '[bar] NVARCHAR (255) NULL',
                    '[fooBar] NVARCHAR (255) NULL',
                    '[far] NVARCHAR (255) NULL',
                    '[barFoo] NVARCHAR (255) NULL'
                ],
            ],
            'text columns' => [
                'provided' => [
                    [
                        'type' => 'text',
                        'size' => '1000',
                        'dbName' => 'foo',
                    ],
                    [
                        'type' => 'ntext',
                        'size' => '100000',
                        'dbName' => 'bor',
                    ],
                    [
                        'type' => 'image',
                        'size' => '1000000',
                        'dbName' => 'bar',
                    ],
                ],
                'expected' => [
                    '[foo] NVARCHAR (MAX) NULL',
                    '[bor] NVARCHAR (MAX) NULL',
                    '[bar] NVARCHAR (MAX) NULL'
                ],
            ],
        ];
    }

    /**
     * @dataProvider namesProvider
     * @param string $provided
     * @param string $expected
     */
    public function testEscape(string $provided, string $expected): void
    {
        self::assertSame($expected, SQLTransformer::escape($provided));
    }

    public function namesProvider(): array
    {
        return [
            [
                'foo',
                '[foo]',
            ],
            [
                'foo.bar',
                'foo.[bar]',
            ],
            [ // this is probably very incorrect :]
                'foo.bar.kochba',
                'foo.[bar]',
            ],
        ];
    }
}
