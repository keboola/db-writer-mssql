<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\MSSQLQueryBuilder;
use Keboola\DbWriter\Writer\MSSQLWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class MSSQLWriteAdapterDescriptionsTest extends TestCase
{
    /** @var string[] */
    private array $executed = [];

    /** @var string[] */
    private array $fetched = [];

    private TestLogger $logger;

    private MSSQLWriteAdapter $adapter;

    protected function setUp(): void
    {
        $this->executed = [];
        $this->fetched = [];
        $connection = $this->createPartialMock(MSSQLConnection::class, ['exec', 'fetchAll']);
        $connection->method('exec')->willReturnCallback(function (string $sql): void {
            $this->executed[] = $sql;
        });
        $connection->method('fetchAll')->willReturnCallback(function (string $sql): array {
            $this->fetched[] = $sql;
            return [['schema' => 'dbo']];
        });

        $this->logger = new TestLogger();
        $this->adapter = new MSSQLWriteAdapter($connection, new MSSQLQueryBuilder(), $this->logger);
    }

    public function testTableAndColumns(): void
    {
        $this->adapter->applyDescriptions('sales.simple', new TableDescriptions(
            'Table description',
            ['id' => 'Identifier', 'name' => 'Full name'],
        ));

        self::assertSame([], $this->fetched);
        self::assertSame([
            $this->tableStatement('sales', 'simple', '[sales].[simple]', 'Table description'),
            $this->columnStatement('sales', 'simple', '[sales].[simple]', 'id', 'Identifier'),
            $this->columnStatement('sales', 'simple', '[sales].[simple]', 'name', 'Full name'),
        ], $this->executed);
    }

    public function testUnqualifiedTableUsesDefaultSchema(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions('Table description', []));

        self::assertSame(['SELECT SCHEMA_NAME() AS [schema];'], $this->fetched);
        self::assertSame([
            $this->tableStatement('dbo', 'simple', '[dbo].[simple]', 'Table description'),
        ], $this->executed);
    }

    public function testColumnsOnly(): void
    {
        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions(null, ['id' => 'Identifier']));

        self::assertSame([
            $this->columnStatement('dbo', 'simple', '[dbo].[simple]', 'id', 'Identifier'),
        ], $this->executed);
    }

    public function testEmptyTableDescriptionIsSkipped(): void
    {
        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions('', []));

        self::assertSame([], $this->executed);
    }

    public function testQuoting(): void
    {
        $this->adapter->applyDescriptions('dbo.my]table\'s', new TableDescriptions(
            'It\'s a "table" with \\ backslash',
            ['col]x\'' => 'Bob\'s column'],
        ));

        self::assertSame([
            $this->tableStatement(
                'dbo',
                'my]table\'\'s',
                '[dbo].[my]]table\'\'s]',
                'It\'\'s a "table" with \\ backslash',
            ),
            $this->columnStatement('dbo', 'my]table\'\'s', '[dbo].[my]]table\'\'s]', 'col]x\'\'', 'Bob\'\'s column'),
        ], $this->executed);
    }

    public function testUnicodeDescription(): void
    {
        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions('Popis tabuľky 日本', []));

        self::assertSame([
            $this->tableStatement('dbo', 'simple', '[dbo].[simple]', 'Popis tabuľky 日本'),
        ], $this->executed);
    }

    public function testNumericColumnName(): void
    {
        // PHP casts numeric string keys to int at runtime.
        /** @var array<string, string> $columns */
        $columns = ['123' => 'Numeric name'];
        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions(null, $columns));

        self::assertSame([
            $this->columnStatement('dbo', 'simple', '[dbo].[simple]', '123', 'Numeric name'),
        ], $this->executed);
    }

    public function testLongDescriptionsAreTruncated(): void
    {
        $long = str_repeat('č', 3749) . 'ab';
        $truncated = str_repeat('č', 3749) . 'a';

        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions($long, ['id' => $long]));

        self::assertSame([
            $this->tableStatement('dbo', 'simple', '[dbo].[simple]', $truncated),
            $this->columnStatement('dbo', 'simple', '[dbo].[simple]', 'id', $truncated),
        ], $this->executed);
        self::assertTrue($this->logger->hasWarning(
            'Description of table "dbo.simple" is longer than 3750 characters and was truncated.',
        ));
        self::assertTrue($this->logger->hasWarning(
            'Description of column "id" in table "dbo.simple" is longer than 3750 characters and was truncated.',
        ));
    }

    public function testTruncationCountsUtf16CodeUnits(): void
    {
        // Each emoji is a surrogate pair, i.e. two UTF-16 code units.
        $long = 'a' . str_repeat('😀', 1875);
        $truncated = 'a' . str_repeat('😀', 1874);

        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions($long, []));

        self::assertSame([
            $this->tableStatement('dbo', 'simple', '[dbo].[simple]', $truncated),
        ], $this->executed);
        self::assertTrue($this->logger->hasWarningRecords());
    }

    public function testDescriptionAtLimitIsNotTruncated(): void
    {
        $description = str_repeat('a', 3750);

        $this->adapter->applyDescriptions('dbo.simple', new TableDescriptions($description, []));

        self::assertSame([
            $this->tableStatement('dbo', 'simple', '[dbo].[simple]', $description),
        ], $this->executed);
        self::assertFalse($this->logger->hasWarningRecords());
    }

    private function tableStatement(string $schema, string $table, string $object, string $value): string
    {
        $args = sprintf(
            "@name = N'MS_Description', @value = N'%s', @level0type = N'SCHEMA', @level0name = N'%s', "
            . "@level1type = N'TABLE', @level1name = N'%s'",
            $value,
            $schema,
            $table,
        );
        return sprintf(
            'IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(N\'%s\') '
            . "AND minor_id = 0 AND name = N'MS_Description') "
            . 'EXEC sys.sp_updateextendedproperty %s ELSE EXEC sys.sp_addextendedproperty %s',
            $object,
            $args,
            $args,
        );
    }

    private function columnStatement(
        string $schema,
        string $table,
        string $object,
        string $column,
        string $value,
    ): string {
        $args = sprintf(
            "@name = N'MS_Description', @value = N'%s', @level0type = N'SCHEMA', @level0name = N'%s', "
            . "@level1type = N'TABLE', @level1name = N'%s', @level2type = N'COLUMN', @level2name = N'%s'",
            $value,
            $schema,
            $table,
            $column,
        );
        return sprintf(
            'IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(N\'%1$s\') '
            . "AND minor_id = COLUMNPROPERTY(OBJECT_ID(N'%1\$s'), N'%2\$s', 'ColumnId') AND name = N'MS_Description') "
            . 'EXEC sys.sp_updateextendedproperty %3$s ELSE EXEC sys.sp_addextendedproperty %3$s',
            $object,
            $column,
            $args,
        );
    }
}
