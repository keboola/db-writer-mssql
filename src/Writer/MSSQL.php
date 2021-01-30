<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\MSSQL\CSV\Preprocessor;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class MSSQL extends Writer implements WriterInterface
{

    private const RETRY_MAX_ATTEMPS = 5;

    /** @var array */
    private static $allowedTypes = [
        'int', 'smallint', 'bigint', 'money',
        'decimal', 'real', 'float',
        'date', 'datetime', 'datetime2', 'smalldatetime', 'time', 'timestamp',
        'char', 'varchar', 'text',
        'nchar', 'nvarchar', 'ntext',
        'binary', 'varbinary', 'image',
    ];

    /** @var array */
    private static $typesWithSize = [
        'identity',
        'decimal', 'float',
        'datetime', 'time',
        'char', 'varchar',
        'nchar', 'nvarchar',
        'binary', 'varbinary',
    ];

    /** @var \PDO */
    protected $db;

    public function createConnection(array $dbParams): \PDO
    {
        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!array_key_exists($r, $dbParams)) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        // construct DSN connection string
        $host = $dbParams['host'];
        $host .= (isset($dbParams['port']) && $dbParams['port'] !== '1433') ? ',' . $dbParams['port'] : '';
        $host .= empty($dbParams['instance']) ? '' : '\\\\' . $dbParams['instance'];

        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $dbParams['database'];

        $dsn = sprintf('sqlsrv:%s', implode(';', $options));

        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private function bcpCreateStage(array $table): void
    {
        $sqlColumns = SQLTransformer::convertColumns($table['items']);
        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s)',
            SQLTransformer::escape($table['dbName']),
            implode(',', $sqlColumns)
        ));
    }

    public function write(CsvFile $csv, array $table): void
    {
        $version = $this->getSqlServerVersion();

        $preprocessor = new Preprocessor($csv);
        $filename = $preprocessor->process();

        $this->logger->info('BCP import started');
        $dstTableName = $table['dbName'];

        // create staging table
        $stagingTable = $table;
        $stagingTable['dbName'] = $this->prefixTableName(uniqid('stage_') . '_', $dstTableName);
        $this->drop($stagingTable['dbName']);
        $this->bcpCreateStage($stagingTable);
        $this->logger->info('BCP staging table created');

        // insert into staging usig BCP
        $this->logger->info('BCP importing to staging table');

        $proxy = new RetryProxy(
            new SimpleRetryPolicy(self::RETRY_MAX_ATTEMPS, [UserException::class]),
            new ExponentialBackOffPolicy(),
            $this->logger
        );

        $proxy->call(function () use ($filename, $stagingTable): void {
            $bcp = new BCP($this->db, $this->dbParams, $this->logger);
            $bcp->import($filename, $stagingTable);
        });

        // move to destination table
        $this->logger->info('BCP moving to destination table');
        $columns = [];
        foreach ($table['items'] as $col) {
            $type = strtolower($col['type']);
            $colName = SQLTransformer::escape($col['dbName']);
            $size = !empty($col['size'])?'('.$col['size'].')':'';
            $srcColName = $colName;
            if (!empty($col['nullable'])) {
                $srcColName = sprintf("NULLIF(%s, '')", $colName);
            }
            $column = $this->bcpCast($srcColName, $type, $size, $colName, $version);
            $columns[] = $column;
        }

        $query = sprintf(
            'INSERT INTO %s SELECT %s FROM %s',
            SQLTransformer::escape($dstTableName),
            implode(',', $columns),
            SQLTransformer::escape($stagingTable['dbName'])
        );

        $this->execQuery($query);
        $this->logger->info('BCP data moved to destination table');

        // drop staging
        $this->drop($stagingTable['dbName']);
        $this->logger->info('BCP staging table dropped');
        $this->logger->info('BCP import finished');
    }

    public function bcpCast(string $srcColName, string $type, string $size, string $colName, int $version): string
    {
        if ($version > 10) {
            return sprintf('TRY_CAST(%s AS %s%s) as %s', $srcColName, $type, $size, $colName);
        }

        return sprintf('CAST(%s AS %s%s) as %s', $srcColName, $type, $size, $colName);
    }

    public function drop(string $tableName): void
    {
        $sql = sprintf(
            "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
            $tableName,
            SQLTransformer::escape($tableName)
        );
        $this->execQuery($sql);
    }

    public function truncate(string $tableName): void
    {
        $sql = sprintf(
            "IF OBJECT_ID('%s', 'U') IS NOT NULL TRUNCATE TABLE %s",
            $tableName,
            SQLTransformer::escape($tableName)
        );
        $this->execQuery($sql);
    }

    public function create(array $table): void
    {
        $columnsSql = [];
        foreach ($table['items'] as $k => $col) {
            $type = strtolower($col['type']);
            if ($type === 'ignore') {
                continue;
            }

            if (!empty($col['size']) && in_array($type, self::$typesWithSize)) {
                $type .= "({$col['size']})";
            }

            $null = empty($col['nullable']) ? 'NOT NULL' : 'NULL';

            $default = empty($col['default']) ? '' : $col['default'];
            if ($type === 'text') {
                $default = '';
            }

            $columnsSql[] = sprintf("%s %s %s %s", SQLTransformer::escape($col['dbName']), $type, $null, $default);
        }

        $pkSql = '';
        if (!empty($table['primaryKey'])) {
            $constraintId = uniqid(sprintf(
                'PK_%s_%s',
                str_replace('.', '_', $table['dbName']),
                implode('_', $table['primaryKey'])
            ));
            $pkSql = sprintf(
                'CONSTRAINT [%s] PRIMARY KEY CLUSTERED (%s)',
                $constraintId,
                implode(',', $table['primaryKey'])
            );
        }

        $sql = sprintf(
            'CREATE TABLE %s (%s, %s)',
            SQLTransformer::escape($table['dbName']),
            implode(',', $columnsSql),
            $pkSql
        );

        $this->execQuery($sql);
    }

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function upsert(array $table, string $targetTable): void
    {
        $startTime = microtime(true);
        $this->logger->info('Begin UPSERT');
        $sourceTable = SQLTransformer::escape($table['dbName']);
        $targetTable = SQLTransformer::escape($targetTable);

        // disable indices
        $this->modifyIndices($targetTable, 'disable');

        $columns = array_filter($table['items'], function ($item) {
            return $item['type'] !== 'IGNORE';
        });

        $columns = array_map(function ($item) {
            return SQLTransformer::escape($item['dbName']);
        }, $columns);

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = "a.{$value}=b.{$value}";
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = "a.{$column}=b.{$column}";
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "UPDATE a
                SET {$valuesClause}
                FROM {$sourceTable} b, {$targetTable} a
                WHERE {$joinClause}
            ";

            $this->execQuery($query);
            $this->logger->info('Data updated');

            // delete updated from temp table
            $query = "DELETE a FROM {$sourceTable} a
                INNER JOIN {$targetTable} b ON {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);
        $this->logger->info('New data inserted');

        $endTime = microtime(true);
        $this->logger->info(sprintf('Finished UPSERT after %s seconds', intval($endTime - $startTime)));

        // enable indices
        $this->modifyIndices($targetTable, 'rebuild');

        // drop temp table
        $this->drop($table['dbName']);
    }

    /**
     * @param $tableName
     * @param $action - DISABLE or REBUILD
     * @throws ApplicationException
     * @internal param string $table
     */
    public function modifyIndices(string $tableName, string $action): void
    {
        if (!in_array(strtoupper($action), ['DISABLE', 'REBUILD'])) {
            throw new ApplicationException('Allowed actions are REBUILD and DISABLE');
        }

        $stmt = $this->db->query(sprintf("
            select I.name 
            from sys.indexes I
            inner join sys.tables T on I.object_id = T.object_id
            where I.type_desc = 'NONCLUSTERED' and T.name = '%s'
            and I.name is not null
        ", $tableName));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        if (!empty($res)) {
            foreach ($res as $index) {
                $this->db->query(sprintf(
                    'ALTER INDEX %s ON %s %s',
                    $index['name'],
                    SQLTransformer::escape($tableName),
                    strtoupper($action)
                ));
            }
        }
    }

    public function tableExists(string $tableName): bool
    {
        $tableArr = explode('.', $tableName);
        $tableName = isset($tableArr[1])?$tableArr[1]:$tableArr[0];
        $tableName = str_replace(['[',']'], '', $tableName);
        $stmt = $this->db->query(
            sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName)
        );
        $res = $stmt->fetchAll();

        return !empty($res);
    }

    private function execQuery(string $query): void
    {
        $this->logger->info(sprintf("Executing query: '%s'", $query));

        $retryPolicy = new SimpleRetryPolicy(self::RETRY_MAX_ATTEMPS, [\PDOException::class]);
        $backOffPolicy = new ExponentialBackOffPolicy(100);
        $proxy = new RetryProxy($retryPolicy, $backOffPolicy, $this->logger);

        try {
            $proxy->call(function () use ($query): void {
                try {
                    $this->db->exec($query);
                } catch (\PDOException $e) {
                    // Reconnect
                    try {
                        $this->db = $this->createConnection($this->dbParams);
                    } catch (\Throwable $e) {
                        // ignore reconnect error
                    }

                    throw $e;
                }
            });
        } catch (\PDOException $e) {
            throw $this->createUserException($e, $query);
        }
    }

    public function showTables(string $dbName): array
    {
        throw new \Exception('Not implemented');
    }

    public function getTableInfo(string $tableName): array
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            $schema = $tableNameArr[0];
            $tableName = $tableNameArr[1];
        }

        // phpcs:disable Generic.Files.LineLength
        $sql = sprintf("
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
        ", $tableName);
        // phpcs:enable

        if (!empty($schema)) {
            $sql .= sprintf(" AND TABLE_SCHEMA='%s'", $schema);
        }

        $stmt = $this->db->query($sql);
        $columns = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return [
            'columns' => $columns,
        ];
    }

    public function testConnection(): void
    {
        $this->db->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    private function createUserException(\Throwable $e, string $query = ''): UserException
    {
        $message = sprintf('DB query failed: %s', $e->getMessage());
        return new UserException($message, 0, $e, ['query' => $query]);
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

    public function validateTable(array $tableConfig): void
    {
        $dbColumns = $this->getTableInfo($tableConfig['dbName'])['columns'];

        foreach ($tableConfig['items'] as $column) {
            $exists = false;
            $targetDataType = null;
            foreach ($dbColumns as $dbColumn) {
                $exists = ($dbColumn['COLUMN_NAME'] === $column['dbName']);
                if ($exists) {
                    $targetDataType = $dbColumn['DATA_TYPE'];
                    break;
                }
            }

            if (!$exists) {
                throw new UserException(sprintf(
                    'Column \'%s\' not found in destination table \'%s\'',
                    $column['dbName'],
                    $tableConfig['dbName']
                ));
            }

            if ($targetDataType !== strtolower($column['type'])) {
                throw new UserException(sprintf(
                    // phpcs:ignore
                    'Data type mismatch. Column \'%s\' is of type \'%s\' in writer, but is \'%s\' in destination table \'%s\'',
                    $column['dbName'],
                    $column['type'],
                    $targetDataType,
                    $tableConfig['dbName']
                ));
            }
        }
    }

    private function getSqlServerVersion(): int
    {
        // get the MSSQL Server version (note, 2008 is version 10.*)
        $res = $this->db->query("SELECT SERVERPROPERTY('ProductVersion') AS version;");
        $versionString = $res->fetch(\PDO::FETCH_ASSOC);
        if (!isset($versionString['version'])) {
            throw new UserException('Unable to get SQL Server Version Information');
        }
        $versionParts = explode('.', $versionString['version']);
        $this->logger->info(
            sprintf('Found database server version: %s', $versionString['version'])
        );
        return (int) $versionParts[0];
    }
}
