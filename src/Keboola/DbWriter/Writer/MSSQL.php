<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\MSSQL\CSV\Preprocessor;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class MSSQL extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'int', 'smallint', 'bigint', 'money',
        'decimal', 'real', 'float',
        'date', 'datetime', 'datetime2', 'smalldatetime', 'time', 'timestamp',
        'char', 'varchar', 'text',
        'nchar', 'nvarchar', 'ntext',
        'binary', 'varbinary', 'image',
    ];

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

    public function createConnection($dbParams)
    {
        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!array_key_exists($r, $dbParams)) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        // construct DSN connection string
        $host = $dbParams['host'];
        $host .= (isset($dbParams['port']) && $dbParams['port'] !== '1433') ? ',' . $dbParams['port'] : '';
        $host .= empty($dbParams['instance']) ? '' : '\\\\' . $dbParams['instance'];

        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $dbParams['database'];

        $dsn = sprintf("sqlsrv:%s", implode(';', $options));

        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // ms sql doesn't support options
        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private function bcpCreateStage($table)
    {
        $sqlColumns = array_map(function ($col) {
            return sprintf(
                "%s VARCHAR (%s) NULL",
                $this->escape($col['dbName']),
                (!empty($col['size']) && strstr(strtolower($col['type']), 'char') !== false) ? $col['size'] : '255'
            );
        }, array_filter($table['items'], function($item) {
            return (strtolower($item['type']) !== 'ignore');
        }));

        $this->execQuery(sprintf(
            "CREATE TABLE %s (%s)",
            $this->escape($table['dbName']),
            implode(',', $sqlColumns)
        ));
    }

    public function write(CsvFile $csv, array $table)
    {
        $preprocessor = new Preprocessor($csv);
        $filename = $preprocessor->process();

        $this->logger->info("BCP import started");
        $dstTableName = $table['dbName'];

        // create staging table
        $stagingTable = $table;
        $stagingTable['dbName'] = $this->prefixTableName(uniqid('stage_') . '_', $dstTableName);
        $this->drop($stagingTable['dbName']);
        $this->bcpCreateStage($stagingTable);
        $this->logger->info("BCP staging table created");

        // insert into staging usig BCP
        $this->logger->info("BCP importing to staging table");
        $bcp = new BCP($this->db, $this->dbParams, $this->logger);
        $bcp->import($filename, $stagingTable);

        // move to destination table
        $this->logger->info("BCP moving to destination table");
        $columns = [];
        foreach ($table['items'] as $col) {
            $type = strtolower($col['type']);
            $colName = $this->escape($col['dbName']);
            $size = !empty($col['size'])?'('.$col['size'].')':'';
//            $column = sprintf('CONVERT(%s%s, %s) as %s', $type, $size, $colName, $colName);
            $srcColName = $colName;
            if (!empty($col['nullable'])) {
                $srcColName = sprintf("NULLIF(%s, '')", $colName);
            }
            $column = sprintf('TRY_CAST(%s AS %s%s) as %s', $srcColName, $type, $size, $colName);
            $columns[] = $column;
        }

        $query = sprintf(
            'SELECT %s INTO %s FROM %s',
            implode(',', $columns),
            $this->escape($dstTableName),
            $this->escape($stagingTable['dbName'])
        );
        $this->execQuery($query);
        $this->logger->info("BCP data moved to destination table");

        // drop staging
        $this->drop($stagingTable['dbName']);
        $this->logger->info("BCP staging table dropped");
        $this->logger->info("BCP import finished");
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        return true;
    }

    public function drop($tableName)
    {
        $sql = sprintf(
            "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
            $tableName,
            $this->escape($tableName)
        );
        $this->execQuery($sql);
    }

    public function truncate($tableName)
    {
        $sql = sprintf(
            "IF OBJECT_ID('%s', 'U') IS NOT NULL TRUNCATE TABLE %s",
            $tableName,
            $this->escape($tableName)
        );
        $this->execQuery($sql);
    }

    private function escape($obj)
    {
        $objNameArr = explode('.', $obj);
        if (count($objNameArr) > 1) {
            return $objNameArr[0] . ".[" . $objNameArr[1] . "]";
        }

        return "[" . $objNameArr[0] . "]";
    }

    public function create(array $table)
    {
        $columnsSql = [];
        foreach ($table['items'] as $k => $col) {
            $type = strtolower($col['type']);
            if ($type == 'ignore') {
                continue;
            }

            if (!empty($col['size']) && in_array($type, self::$typesWithSize)) {
                $type .= "({$col['size']})";
            }

            $null = empty($col['nullable']) ? 'NOT NULL' : 'NULL';

            $default = empty($col['default']) ? '' : $col['default'];
            if ($type == 'text') {
                $default = '';
            }

            $columnsSql[] = "{$this->escape($col['dbName'])} $type $null $default";
        }

        $pkSql = '';
        if (!empty($table['primaryKey'])) {
            $constraintId = uniqid(sprintf(
                "PK_%s_%s",
                str_replace('.', '_', $table['dbName']),
                implode('_', $table['primaryKey'])
            ));
            $pkSql = PHP_EOL . sprintf(
                "CONSTRAINT [%s] PRIMARY KEY CLUSTERED (%s)",
                $constraintId,
                implode(',', $table['primaryKey'])
            ) . PHP_EOL;
        }

        $sql = sprintf("
            CREATE TABLE %s (
              %s 
              %s
            )",
            $this->escape($table['dbName']),
            implode(',', $columnsSql),
            $pkSql
        );

        $this->execQuery($sql);
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function upsert(array $table, $targetTable)
    {
        $startTime = microtime(true);
        $this->logger->info("Begin UPSERT");
        $sourceTable = $this->escape($table['dbName']);
        $targetTable = $this->escape($targetTable);

        // disable indices
        $this->modifyIndices($targetTable, 'disable');

        $columns = array_filter($table['items'], function ($item) {
            return $item['type'] !== 'IGNORE';
        });

        $columns = array_map(function ($item) {
            return $this->escape($item['dbName']);
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
            $this->logger->info("Data updated");

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
        $this->logger->info("New data inserted");

        $endTime = microtime(true);
        $this->logger->info(sprintf("Finished UPSERT after %s seconds", intval($endTime - $startTime)));

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
    public function modifyIndices($tableName, $action)
    {
        if (!in_array(strtoupper($action), ['DISABLE', 'REBUILD'])) {
            throw new ApplicationException("Allowed actions are REBUILD and DISABLE");
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
                    "ALTER INDEX %s ON %s %s",
                    $index['name'],
                    $this->escape($tableName),
                    strtoupper($action)
                ));
            }
        }
    }

    public function tableExists($tableName)
    {
        $tableArr = explode('.', $tableName);
        $tableName = isset($tableArr[1])?$tableArr[1]:$tableArr[0];
        $tableName = str_replace(['[',']'], '', $tableName);
        $stmt = $this->db->query(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        $res = $stmt->fetchAll();

        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query: '%s'", $query));

        $tries = 0;
        $maxTries = 3;
        $exception = null;

        while ($tries < $maxTries) {
            $exception = null;
            try {
                $this->db->exec($query);
                break;
            } catch (\PDOException $e) {
                $exception = $this->handleDbError($e, $query);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            } catch (\ErrorException $e) {
                $exception = $this->handleDbError($e, $query);
                $this->logger->info(sprintf('%s. Retrying... [%dx]', $exception->getMessage(), $tries + 1));
            }
            sleep(pow($tries, 2));
            $this->db = $this->createConnection($this->dbParams);
            $tries++;
        }

        if ($exception) {
            throw $exception;
        }
    }

    public function showTables($dbName)
    {
        throw new \Exception("Not implemented");
    }

    public function getTableInfo($tableName)
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            $schema = $tableNameArr[0];
            $tableName = $tableNameArr[1];
        }

        $sql = sprintf("
          SELECT COLUMN_NAME,* 
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_NAME = '%s'
        ", $tableName);

        if (!empty($schema)) {
            $sql .= sprintf(" AND TABLE_SCHEMA='%s'", $schema);
        }

        $stmt = $this->db->query($sql);
        $columns = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return [
            'columns' => $columns
        ];
    }

    public function testConnection()
    {
        $this->db->query('SELECT GETDATE() AS CurrentDateTime')->execute();
    }

    private function handleDbError(\Exception $e, $query = '')
    {
        $message = sprintf('DB query failed: %s', $e->getMessage());
        $exception = new UserException($message, 0, $e, ['query' => $query]);

        try {
            $this->db = $this->createConnection($this->dbParams);
        } catch (\Exception $e) {
        };
        return $exception;
    }

    public function generateTmpName($tableName)
    {
        return $this->prefixTableName('tmp_', $tableName);
    }

    private function prefixTableName($prefix, $tableName)
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            return $tableNameArr[0] . "." . $prefix . $tableNameArr[1];
        }

        return $prefix . $tableName;
    }

    public function validateTable($tableConfig)
    {
        $dbColumns = $this->getTableInfo($tableConfig['dbName'])['columns'];

        foreach ($tableConfig['items'] as $column) {
            $exists = false;
            $targetDataType = null;
            foreach ($dbColumns as $dbColumn) {
                $exists = ($dbColumn['COLUMN_NAME'] == $column['dbName']);
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
                    'Data type mismatch. Column \'%s\' is of type \'%s\' in writer, but is \'%s\' in destination table \'%s\'',
                    $column['dbName'],
                    $column['type'],
                    $targetDataType,
                    $tableConfig['dbName']
                ));
            }
        }
    }
}
