<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests\Writer;

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\Preprocessor;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class MSSQLConnectionTest extends TestCase
{
    public function testGetConnection(): void
    {
        $databaseConfig = $this->getDatabaseConfig();

        $dsn = sprintf(
            'sqlsrv:Server=%s,%s;Database=%s',
            $databaseConfig->getHost(),
            $databaseConfig->getPort(),
            $databaseConfig->getDatabase(),
        );

        $logger = new TestLogger();
        $connection = new MSSQLConnection(
            $logger,
            $dsn,
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            [],
        );
        $connection->testConnection();

        self::assertInstanceOf(MSSQLConnection::class, $connection);
        self::assertTrue(
            $logger->hasInfo('Creating PDO connection to "sqlsrv:Server=mssql,1433;Database=test".'),
        );
        self::assertTrue(
            $logger->hasDebug('Running query "SELECT GETDATE() AS CurrentDateTime".'),
        );
    }

    public function testQuoteIdentifier(): void
    {
        $databaseConfig = $this->getDatabaseConfig();

        $dsn = sprintf(
            'sqlsrv:Server=%s,%s;Database=%s',
            $databaseConfig->getHost(),
            $databaseConfig->getPort(),
            $databaseConfig->getDatabase(),
        );

        $logger = new TestLogger();
        $connection = new MSSQLConnection(
            $logger,
            $dsn,
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            [],
        );

        $quoteSimple = $connection->quoteIdentifier('test');
        self::assertEquals('[test]', $quoteSimple);

        $quoteWithSchema = $connection->quoteIdentifier('dbo.test');
        self::assertEquals('dbo.[test]', $quoteWithSchema);
    }

    private function getDatabaseConfig(): MSSQLDatabaseConfig
    {
        return new MSSQLDatabaseConfig(
            (string) getenv('DB_HOST'),
            (string) getenv('DB_PORT'),
            (string) getenv('DB_DATABASE'),
            (string) getenv('DB_USER'),
            (string) getenv('DB_PASSWORD'),
            (string) getenv('DB_SCHEMA'),
            null,
            null,
            null,
            null,
            null,
        );
    }
}
