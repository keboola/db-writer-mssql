<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\MSSQLQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;

class MSSQLQueryBuilderTest extends TestCase
{
    public function testTableInfoQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = <<<SQL
SELECT [c].*, [pk_name] 
FROM [INFORMATION_SCHEMA].[COLUMNS] as [c]
LEFT JOIN (
    SELECT [tc].[CONSTRAINT_TYPE], [tc].[TABLE_NAME], [ccu].[COLUMN_NAME], [ccu].[CONSTRAINT_NAME] as [pk_name]
    FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [ccu]
    JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc]
    ON [ccu].[CONSTRAINT_NAME] = [tc].[CONSTRAINT_NAME] AND [ccu].[TABLE_NAME] = [tc].[TABLE_NAME] AND [CONSTRAINT_TYPE] = 'PRIMARY KEY' 
) AS [pk]
ON [pk].[TABLE_NAME] = [c].[TABLE_NAME] AND [pk].[COLUMN_NAME] = [c].[COLUMN_NAME]
WHERE [c].[TABLE_NAME] = 'test_table'
SQL;
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->tableInfoQueryStatement($this->getConnectionMock(), 'test_table'),
        );
    }

    public function testTableInfoQueryWithShema(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = <<<SQL
SELECT [c].*, [pk_name] 
FROM [INFORMATION_SCHEMA].[COLUMNS] as [c]
LEFT JOIN (
    SELECT [tc].[CONSTRAINT_TYPE], [tc].[TABLE_NAME], [ccu].[COLUMN_NAME], [ccu].[CONSTRAINT_NAME] as [pk_name]
    FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [ccu]
    JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc]
    ON [ccu].[CONSTRAINT_NAME] = [tc].[CONSTRAINT_NAME] AND [ccu].[TABLE_NAME] = [tc].[TABLE_NAME] AND [CONSTRAINT_TYPE] = 'PRIMARY KEY' 
) AS [pk]
ON [pk].[TABLE_NAME] = [c].[TABLE_NAME] AND [pk].[COLUMN_NAME] = [c].[COLUMN_NAME]
WHERE [c].[TABLE_NAME] = 'test_table' AND TABLE_SCHEMA='schema'
SQL;
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->tableInfoQueryStatement($this->getConnectionMock(), 'schema.test_table'),
        );
    }

    public function testCreateTableQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();
        // phpcs:disable Generic.Files.LineLength
        $expected = "CREATE TABLE [test_table] ([col2] varchar(255) NOT NULL ,[col3] varchar(255) NULL ,[col4] varchar(255) NULL DEFAULT CAST('default' AS varchar),[col5] varchar(255) NOT NULL DEFAULT CAST('default' AS varchar),CONSTRAINT [%s] PRIMARY KEY CLUSTERED (col1,col2))";
        // phpcs:enable Generic.Files.LineLength

        $exportConfig = $this->getExportConfig();
        self::assertStringMatchesFormat(
            $expected,
            $queryBuilder->createQueryStatement(
                $this->getConnectionMock(),
                'test_table',
                false,
                $exportConfig->getItems(),
                $exportConfig->getPrimaryKey(),
            ),
        );
    }

    public function testUpsertUpdateRowsQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = 'UPDATE a SET a.[col1] = b.[col1],a.[col2] = b.[col2],a.[col3] = b.[col3],a.[col4] = b.[col4],a.[col5] = b.[col5] FROM [test] a INNER JOIN [stage_table_name] b ON a.[col1] = b.[col1] AND a.[col2] = b.[col2];';
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->upsertUpdateRowsQueryStatement(
                $this->getConnectionMock(),
                $this->getExportConfig(),
                'stage_table_name',
            ),
        );
    }

    public function testUpsertDeleteRowsQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = 'DELETE a FROM [stage_table_name] a INNER JOIN [test] b ON a.[col1] = b.[col1] AND a.[col2] = b.[col2]';
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->upsertDeleteRowsQueryStatement(
                $this->getConnectionMock(),
                $this->getExportConfig(),
                'stage_table_name',
            ),
        );
    }

    public function testUpsertQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();
        $expected = 'INSERT INTO [test] ([col1], [col2], [col3], [col4], [col5]) SELECT * FROM [stage_table_name]';

        self::assertSame(
            $expected,
            $queryBuilder->upsertQueryStatement(
                $this->getConnectionMock(),
                $this->getExportConfig(),
                'stage_table_name',
            ),
        );
    }

    public function testTableExistsQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \'table_name\'';
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->tableExistsQueryStatement(
                $this->getConnectionMock(),
                'table_name',
            ),
        );
    }

    public function testTableExistsQueryWithSchema(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = \'table_name\'';
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->tableExistsQueryStatement(
                $this->getConnectionMock(),
                'schema.table_name',
            ),
        );
    }

    public function testDropQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = 'IF OBJECT_ID(\'table_name\', \'U\') IS NOT NULL DROP TABLE [table_name]';
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->dropQueryStatement(
                $this->getConnectionMock(),
                'table_name',
            ),
        );
    }

    public function testWriteDataFromBcpQuery(): void
    {
        $queryBuilder = new MSSQLQueryBuilder();

        // phpcs:disable Generic.Files.LineLength
        $expected = "INSERT INTO [table_name] SELECT TRY_CAST(COALESCE([col2], '') AS varchar(255)) as [col2],TRY_CAST(NULLIF([col3], '') AS varchar(255)) as [col3],TRY_CAST(NULLIF([col4], '') AS varchar(255)) as [col4],TRY_CAST(COALESCE([col5], 'default') AS varchar(255)) as [col5] FROM [stage_table_name]";
        // phpcs:enable Generic.Files.LineLength

        self::assertSame(
            $expected,
            $queryBuilder->writeDataFromBcpQueryStatement(
                $this->getConnectionMock(),
                'table_name',
                'stage_table_name',
                $this->getExportConfig(),
                11,
            ),
        );
    }


    private function getConnectionMock(): MSSQLConnection
    {
        $connection = $this->createMock(MSSQLConnection::class);
        $connection->method('quoteIdentifier')
            ->willReturnCallback(function (string $str): string {
                return '[' . $str . ']';
            });

        return $connection;
    }

    private function getExportConfig(): ExportConfig
    {
        $exportConfig = new ExportConfig(
            '/data',
            'MSSQL',
            new MSSQLDatabaseConfig(
                'host',
                '1433',
                'user',
                'pass',
                'test',
                'schema',
                null,
                null,
                null,
                null,
                null,
            ),
            'test',
            'test',
            true,
            true,
            ['col1', 'col2'],
            [
                new ItemConfig(
                    'col1',
                    'col1',
                    'ignore',
                    '255',
                    null,
                    null,
                ),
                new ItemConfig(
                    'col2',
                    'col2',
                    'varchar',
                    '255',
                    false,
                    null,
                ),
                new ItemConfig(
                    'col3',
                    'col3',
                    'varchar',
                    '255',
                    true,
                    null,
                ),
                new ItemConfig(
                    'col4',
                    'col4',
                    'varchar',
                    '255',
                    true,
                    'default',
                ),
                new ItemConfig(
                    'col5',
                    'col5',
                    'varchar',
                    '255',
                    false,
                    'default',
                ),
            ],
            '/data/test.csv',
        );

        return $exportConfig;
    }
}
