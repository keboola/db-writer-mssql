<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterAdapter\Query\DefaultQueryBuilder;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class MSSQLQueryBuilder extends DefaultQueryBuilder
{
    public function tableInfoQueryStatement(Connection $connection, string $dbName): string
    {
        $dbNameArr = explode('.', $dbName);
        if (count($dbNameArr) > 1) {
            $schema = $dbNameArr[0];
            $tableName = $dbNameArr[1];
        } else {
            $schema = null;
            $tableName = $dbName;
        }

        // phpcs:disable Generic.Files.LineLength
        $sql = <<<SQL
SELECT [c].*, [pk_name] 
FROM [INFORMATION_SCHEMA].[COLUMNS] as [c]
LEFT JOIN (
    SELECT [tc].[CONSTRAINT_TYPE], [tc].[TABLE_NAME], [ccu].[COLUMN_NAME], [ccu].[CONSTRAINT_NAME] as [pk_name]
    FROM [INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] AS [ccu]
    JOIN [INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] AS [tc]
    ON [ccu].[CONSTRAINT_NAME] = [tc].[CONSTRAINT_NAME] AND [ccu].[TABLE_NAME] = [tc].[TABLE_NAME] AND [CONSTRAINT_TYPE] = 'PRIMARY KEY' 
) AS [pk]
ON [pk].[TABLE_NAME] = [c].[TABLE_NAME] AND [pk].[COLUMN_NAME] = [c].[COLUMN_NAME]
WHERE [c].[TABLE_NAME] = '%s'
SQL;

        $sql = sprintf($sql, $tableName);
        // phpcs:enable

        if (!empty($schema)) {
            $sql .= sprintf(" AND TABLE_SCHEMA='%s'", $schema);
        }

        return $sql;
    }

    /**
     * @param ItemConfig[] $items
     * @param string[] $primaryKeys
     */
    public function createQueryStatement(
        Connection $connection,
        string $tableName,
        bool $isTempTable,
        array $items,
        ?array $primaryKeys = null,
    ): string {
        $createTable = sprintf(
            'CREATE TABLE %s',
            $connection->quoteIdentifier($tableName),
        );

        $filteredItems = array_filter(
            $items,
            function (ItemConfig $itemConfig) {
                return strtolower($itemConfig->getType()) !== 'ignore';
            },
        );

        $columnsDefinition = array_map(
            function (ItemConfig $itemConfig) use ($connection) {
                $type = strtolower($itemConfig->getType());
                return sprintf(
                    '%s %s%s %s %s',
                    $connection->quoteIdentifier($itemConfig->getDbName()),
                    $type,
                    $itemConfig->hasSize() ? sprintf('(%s)', $itemConfig->getSize()) : '',
                    $itemConfig->getNullable() ? 'NULL' : 'NOT NULL',
                    $itemConfig->hasDefault() && $itemConfig->getType() !== 'TEXT' ?
                        sprintf(
                            'DEFAULT CAST(\'%s\' AS %s)',
                            addslashes($itemConfig->getDefault()),
                            $type,
                        ) :
                        '',
                );
            },
            $filteredItems,
        );

        if ($primaryKeys) {
            $constraintId = uniqid(sprintf(
                'PK_%s_%s',
                str_replace('.', '_', $tableName),
                implode('_', $primaryKeys),
            ));
            $columnsDefinition[] = sprintf(
                'CONSTRAINT %s PRIMARY KEY CLUSTERED (%s)',
                $connection->quoteIdentifier($constraintId),
                implode(',', $primaryKeys),
            );
        }

        return sprintf(
            '%s (%s)',
            $createTable,
            implode(',', $columnsDefinition),
        );
    }

    public function upsertUpdateRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $columns = array_map(function ($item) {
            return $item->getDbName();
        }, $exportConfig->getItems());

        // update data
        $joinClauseArr = array_map(fn($item) => sprintf(
            'a.%s = b.%s',
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        $valuesClauseArr = array_map(fn($item) => sprintf(
            'a.%s = b.%s',
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($item),
        ), $columns);
        $valuesClause = implode(',', $valuesClauseArr);

        return sprintf(
            'UPDATE a SET %s FROM %s a INNER JOIN %s b ON %s;',
            $valuesClause,
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $connection->quoteIdentifier($stageTableName),
            $joinClause,
        );
    }

    public function upsertDeleteRowsQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $joinClauseArr = array_map(fn($item) => sprintf(
            'a.%s = b.%s',
            $connection->quoteIdentifier($item),
            $connection->quoteIdentifier($item),
        ), $exportConfig->getPrimaryKey());
        $joinClause = implode(' AND ', $joinClauseArr);

        return sprintf(
            'DELETE a FROM %s a INNER JOIN %s b ON %s',
            $connection->quoteIdentifier($stageTableName),
            $connection->quoteIdentifier($exportConfig->getDbName()),
            $joinClause,
        );
    }

    public function upsertQueryStatement(
        Connection $connection,
        ExportConfig $exportConfig,
        string $stageTableName,
    ): string {
        $columns = $this->quotedDbColumnNames($exportConfig->getItems(), $connection);

        return sprintf(
            'INSERT INTO %s (%s) SELECT * FROM %s',
            $connection->quoteIdentifier($exportConfig->getDbName()),
            implode(', ', $columns),
            $connection->quoteIdentifier($stageTableName),
        );
    }

    public function tableExistsQueryStatement(Connection $connection, string $tableName): string
    {
        $tableArr = explode('.', $tableName);
        $tableName = $tableArr[1] ?? $tableArr[0];
        $tableName = str_replace(['[', ']'], '', $tableName);
        return sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName);
    }

    public function dropQueryStatement(Connection $connection, string $tableName): string
    {
        return sprintf(
            "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
            $tableName,
            $connection->quoteIdentifier($tableName),
        );
    }

    public function writeDataFromBcpQueryStatement(
        Connection $connection,
        string $tableName,
        string $stageTableName,
        ExportConfig $exportConfig,
        int $version,
    ): string {
        $filteredItems = array_filter(
            $exportConfig->getItems(),
            function (ItemConfig $itemConfig) {
                return strtolower($itemConfig->getType()) !== 'ignore';
            },
        );

        $columns = array_map(
            function (ItemConfig $itemConfig) use ($connection, $version) {
                $name = $connection->quoteIdentifier($itemConfig->getDbName());
                $srcName = $name;
                if ($itemConfig->getNullable()) {
                    $srcName = sprintf("NULLIF(%s, '')", $srcName);
                }

                return $this->bcpCast(
                    srcColName: $srcName,
                    type: strtolower($itemConfig->getType()),
                    size: $itemConfig->hasSize() ? sprintf('(%s)', $itemConfig->getSize()) : '',
                    colName: $name,
                    nullable: $itemConfig->getNullable(),
                    default: $itemConfig->hasDefault() ? $itemConfig->getDefault() : '',
                    version: $version,
                );
            },
            $filteredItems,
        );

        return sprintf(
            'INSERT INTO %s SELECT %s FROM %s',
            $connection->quoteIdentifier($tableName),
            implode(',', $columns),
            $connection->quoteIdentifier($stageTableName),
        );
    }

    private function bcpCast(
        string $srcColName,
        string $type,
        string $size,
        string $colName,
        bool $nullable,
        string $default,
        int $version,
    ): string {
        // Binary types
        if (in_array($type, BCPHelper::BINARY_TYPES, true)) {
            $convertFunction = $version > 10 ? 'TRY_CONVERT' : 'CONVERT';

            // If image, we have to convert data to varbinary
            if ($type === 'image') {
                $type = 'varbinary';
                $size = '(max)';
            }

            // Last parameter of the convert function is style:
            // 0 - converted as string
            // 1 - converted as hex value (if starts with 0x and length is even number
            // Note: MsSQL cannot convert hex values with odd length
            $style = sprintf(
                'CASE WHEN LEFT(%s, 2) = \'0x\' AND LEN(%s) %% 2 = 0  THEN 1 ELSE 0 END',
                $colName,
                $colName,
            );
            $sql = sprintf('%s(%s%s, %s, %s)', $convertFunction, $type, $size, $colName, $style);

            // If not nullable, use empty binary value 0x instead of the null
            if (!$nullable) {
                $sql = "COALESCE($sql, 0x)";
            }

            $sql .= ' as ' . $colName;
            return $sql;
        }

        // Other types
        $castFunction = $version > 10 ? 'TRY_CAST' : 'CAST';

        // Null must be converted to empty string, because TRY_CAST(NULL as VARCHAR(255)) is NULL
        $rawValue = !$nullable && (in_array($type, BCPHelper::STRING_TYPES) || $default !== '') ?
            "COALESCE($srcColName, '" . addslashes($default) . "')" : $srcColName;

        return sprintf('%s(%s AS %s%s) as %s', $castFunction, $rawValue, $type, $size, $colName);
    }
}
