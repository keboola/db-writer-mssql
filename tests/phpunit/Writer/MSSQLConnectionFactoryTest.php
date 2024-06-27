<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests\Writer;

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\MSSQLConnectionFactory;
use Keboola\DbWriter\Writer\Preprocessor;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class MSSQLConnectionFactoryTest extends TestCase
{
    public function testCreateConnection(): void
    {
        $connectionFactory = new MSSQLConnectionFactory(new TestLogger());
        $connection = $connectionFactory->create($this->getDatabaseConfig());

        self::assertInstanceOf(MSSQLConnection::class, $connection);
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
