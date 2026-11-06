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
use Keboola\DbWriterAdapter\DescriptionWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * @property MSSQLQueryBuilder $queryBuilder
 * @property MSSQLConnection $connection
 */
class MSSQLWriteAdapter extends BaseWriteAdapter implements DescriptionWriteAdapter
{
    private const RETRY_MAX_ATTEMPS = 5;

    private const DESCRIPTION_PROPERTY = 'MS_Description';

    // Extended property values are sql_variant, max 7,500 bytes = 3,750 UTF-16 characters.
    private const DESCRIPTION_MAX_LENGTH = 3750;

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

    public function applyDescriptions(string $tableName, TableDescriptions $descriptions): void
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            [$schema, $table] = $tableNameArr;
        } else {
            $schema = $this->getDefaultSchema();
            $table = $tableName;
        }

        $tableDescription = $descriptions->getTable();
        if ($tableDescription !== null && $tableDescription !== '') {
            $this->logger->info(sprintf('Setting description on table "%s"', $tableName));
            $this->connection->exec($this->upsertDescriptionStatement(
                $schema,
                $table,
                null,
                $this->truncateDescription($tableDescription, $tableName, null),
            ));
        }

        foreach ($descriptions->getColumns() as $column => $description) {
            $column = (string) $column;
            $this->logger->info(sprintf('Setting description on column "%s" in table "%s"', $column, $tableName));
            $this->connection->exec($this->upsertDescriptionStatement(
                $schema,
                $table,
                $column,
                $this->truncateDescription($description, $tableName, $column),
            ));
        }
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

    private function getDefaultSchema(): string
    {
        // Unqualified table names resolve to the user's default schema.
        $result = $this->connection->fetchAll('SELECT SCHEMA_NAME() AS [schema];')[0] ?? [];
        if (!isset($result['schema']) || !is_string($result['schema'])) {
            throw new UserException('Unable to resolve the default schema of the database user.');
        }
        return $result['schema'];
    }

    private function upsertDescriptionStatement(
        string $schema,
        string $table,
        ?string $column,
        string $description,
    ): string {
        $objectId = sprintf(
            'OBJECT_ID(%s)',
            $this->quoteUnicode($this->quoteName($schema) . '.' . $this->quoteName($table)),
        );
        $minorId = $column === null
            ? '0'
            : sprintf('COLUMNPROPERTY(%s, %s, \'ColumnId\')', $objectId, $this->quoteUnicode($column));

        $arguments = sprintf(
            '@name = %s, @value = %s, @level0type = N\'SCHEMA\', @level0name = %s, '
            . '@level1type = N\'TABLE\', @level1name = %s',
            $this->quoteUnicode(self::DESCRIPTION_PROPERTY),
            $this->quoteUnicode($description),
            $this->quoteUnicode($schema),
            $this->quoteUnicode($table),
        );
        if ($column !== null) {
            $arguments .= sprintf(', @level2type = N\'COLUMN\', @level2name = %s', $this->quoteUnicode($column));
        }

        return sprintf(
            'IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = %s '
            . 'AND minor_id = %s AND name = %s) '
            . 'EXEC sys.sp_updateextendedproperty %s '
            . 'ELSE EXEC sys.sp_addextendedproperty %s',
            $objectId,
            $minorId,
            $this->quoteUnicode(self::DESCRIPTION_PROPERTY),
            $arguments,
            $arguments,
        );
    }

    private function truncateDescription(string $description, string $tableName, ?string $column): string
    {
        // SQL Server counts nvarchar length in UTF-16 code units, not code points.
        $utf16 = (string) mb_convert_encoding($description, 'UTF-16LE', 'UTF-8');
        if (strlen($utf16) <= self::DESCRIPTION_MAX_LENGTH * 2) {
            return $description;
        }
        $this->logger->warning(sprintf(
            'Description of %s is longer than %d characters and was truncated.',
            $column === null
                ? sprintf('table "%s"', $tableName)
                : sprintf('column "%s" in table "%s"', $column, $tableName),
            self::DESCRIPTION_MAX_LENGTH,
        ));
        $truncated = substr($utf16, 0, self::DESCRIPTION_MAX_LENGTH * 2);
        // Drop a dangling high surrogate so the result stays valid.
        $lastUnit = unpack('v', substr($truncated, -2))[1] ?? 0;
        if ($lastUnit >= 0xD800 && $lastUnit <= 0xDBFF) {
            $truncated = substr($truncated, 0, -2);
        }
        return (string) mb_convert_encoding($truncated, 'UTF-8', 'UTF-16LE');
    }

    private function quoteUnicode(string $value): string
    {
        return "N'" . str_replace("'", "''", $value) . "'";
    }

    private function quoteName(string $name): string
    {
        return '[' . str_replace(']', ']]', $name) . ']';
    }
}
