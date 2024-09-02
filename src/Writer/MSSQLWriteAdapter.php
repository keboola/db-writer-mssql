<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Exception;
use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\BaseWriteAdapter;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * @property MSSQLQueryBuilder $queryBuilder
 * @property MSSQLConnection $connection
 */
class MSSQLWriteAdapter extends BaseWriteAdapter
{
    private const RETRY_MAX_ATTEMPS = 5;

    public function writeData(string $tableName, ExportConfig $exportConfig): void
    {
        $preprocessor = new Preprocessor(
            new CsvReader($exportConfig->getTableFilePath()),
            $exportConfig->getItems(),
        );
        $filename = $preprocessor->process($tableName);

        $this->logger->info('BCP import started');
        // create staging table
        $stagingTableName = $this->prefixTableName(uniqid('stage_') . '_', $tableName);

        $retryProxy = new RetryProxy(
            new SimpleRetryPolicy(self::RETRY_MAX_ATTEMPS, [UserException::class]),
            new ExponentialBackOffPolicy(),
            $this->logger,
        );
        try {
            $retryProxy->call(function () use ($stagingTableName, $filename, $exportConfig): void {
                /** @var MSSQLDatabaseConfig $databaseConfig */
                $databaseConfig = $exportConfig->getDatabaseConfig();
                $bcp = new BCP($this->connection, $databaseConfig, $this->logger);
                $bcpHelper = new BCPHelper($this->connection);

                $this->drop($stagingTableName);
                $this->create($stagingTableName, false, $bcpHelper->convertColumns($exportConfig->getItems()));
                $this->logger->info('BCP staging table created');

                $this->logger->info('BCP importing to staging table');
                $bcp->import($filename, $stagingTableName, $exportConfig->getItems());
                $this->logger->info('BCP data imported to staging table');
            });

            $this->logger->info('BCP moving to destination table');

            $this->connection->exec(
                $this->queryBuilder->writeDataFromBcpQueryStatement(
                    $this->connection,
                    $tableName,
                    $stagingTableName,
                    $exportConfig,
                    $this->getSqlServerVersion(),
                ),
            );
            $this->logger->info('BCP data moved to destination table');
        } finally {
            $this->drop($stagingTableName);
            $this->logger->info('BCP staging table dropped');
        }
        $this->logger->info('BCP import finished');
    }

    public function upsert(ExportConfig $exportConfig, string $stageTableName): void
    {
        $this->modifyIndices($exportConfig->getDbName(), 'disable');
        parent::upsert($exportConfig, $stageTableName);
        $this->modifyIndices($exportConfig->getDbName(), 'rebuild');
    }

    /**
     * @return string[]
     */
    public function showTables(): array
    {
        throw new Exception('Not implemented');
    }

    /**
     * @return array{Field: string, Type: string}[]
     */
    public function getTableInfo(string $tableName): array
    {
        /** @var array{"COLUMN_NAME": string, "DATA_TYPE": string}[] $res */
        $res = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
            Connection::DEFAULT_MAX_RETRIES,
        );

        return array_map(fn(array $item) => [
            'Field' => $item['COLUMN_NAME'],
            'Type' => $item['DATA_TYPE'],
        ], $res);
    }

    public function generateTmpName(string $tableName): string
    {
        return $this->prefixTableName('tmp_', $tableName);
    }

    private function prefixTableName(string $prefix, string $tableName): string
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            return $tableNameArr[0] . '.' . $prefix . $tableNameArr[1];
        }

        return $prefix . $tableName;
    }

    private function modifyIndices(string $tableName, string $action): void
    {
        if (!in_array(strtoupper($action), ['DISABLE', 'REBUILD'])) {
            throw new ApplicationException('Allowed actions are REBUILD and DISABLE');
        }

        $selectIndexes = <<<SQL
select I.name 
from sys.indexes I
inner join sys.tables T on I.object_id = T.object_id
where I.type_desc = 'NONCLUSTERED' and T.name = '%s'
and I.name is not null
SQL;

        $alterIndex = <<<SQL
ALTER INDEX %s ON %s %s;
SQL;

        $res = $this->connection->fetchAll(sprintf($selectIndexes, $tableName));
        /** @var array{"name": string} $index */
        foreach ($res as $index) {
            $this->connection->exec(sprintf(
                $alterIndex,
                $index['name'],
                $this->connection->quoteIdentifier($tableName),
                strtoupper($action),
            ));
        }
    }

    private function getSqlServerVersion(): int
    {
        // get the MSSQL Server version (note, 2008 is version 10.*)
        $versionString = $this->connection->fetchAll(
            "SELECT SERVERPROPERTY('ProductVersion') AS version;",
        )[0] ?? [];
        if (!isset($versionString['version']) || !is_string($versionString['version'])) {
            throw new UserException('Unable to get SQL Server Version Information');
        }
        $versionParts = explode('.', $versionString['version']);
        $this->logger->info(sprintf('Found database server version: %s', $versionString['version']));
        return (int) $versionParts[0];
    }
}
