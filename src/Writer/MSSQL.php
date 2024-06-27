<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\WriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;

/**
 * @property MSSQLConnection $connection
 */
class MSSQL extends BaseWriter
{
    /**
     * @param MSSQLDatabaseConfig $databaseConfig
     */
    protected function createConnection(DatabaseConfig $databaseConfig): Connection
    {
        $connectionFactory = new MSSQLConnectionFactory($this->logger);
        return $connectionFactory->create($databaseConfig);
    }

    protected function createWriteAdapter(): WriteAdapter
    {
        return new MSSQLWriteAdapter(
            $this->connection,
            new MSSQLQueryBuilder(),
            $this->logger,
        );
    }

    protected function writeIncremental(ExportConfig $exportConfig): void
    {
        parent::writeIncremental($exportConfig);
        $this->adapter->drop(
            $this->adapter->generateTmpName($exportConfig->getDbName()),
        );
    }
}
