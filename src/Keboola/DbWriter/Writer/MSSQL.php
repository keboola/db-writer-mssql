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
use Keboola\DbWriter\Logger;
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

    /** @var Logger */
    protected $logger;

    private $dbParams;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->dbParams = $dbParams;
        $this->logger = $logger;
    }

    private function setTdsVersion($version)
    {
        $tdsConfPath = '/etc/freetds.conf';
        $tdsConf = file_get_contents($tdsConfPath);
        return file_put_contents($tdsConfPath, str_replace('%%TDS_VERSION%%', $version, $tdsConf));
    }

    public function createConnection($dbParams)
    {
        // check params
        foreach (['host', 'database', 'user', '#password'] as $r) {
            if (!array_key_exists($r, $dbParams)) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $tdsVersion = isset($dbParams['tdsVersion'])?$dbParams['tdsVersion']:'7.1';
        $this->setTdsVersion($tdsVersion);

        $port = isset($dbParams['port']) ? $dbParams['port'] : '1433';

        if ($port == '1433') {
            $dsn = sprintf(
                "dblib:host=%s;dbname=%s;charset=UTF-8",
                $dbParams['host'],
                $dbParams['database']
            );
        } else {
            $dsn = sprintf(
                "dblib:host=%s:%s;dbname=%s;charset=UTF-8",
                $dbParams['host'],
                $port,
                $dbParams['database']
            );
        }

        $this->logger->info("Connecting to DSN '" . $dsn . "'");

        // mssql dont support options
        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }

    private function bcpCreateStage($table)
    {
        $tableName = $this->escape($table['dbName']);
        $sql = sprintf("create table %s (", $tableName);

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
            $type = strtolower($col['type']);
            if ($type == 'ignore') {
                continue;
            }
            $sql .= "{$this->escape($col['dbName'])} varchar (255) NULL";
            $sql .= ', ';
        }
        $sql = substr($sql, 0, -1);
        $sql .= ")" . PHP_EOL;

        $this->logger->info(sprintf("Executing query: '%s'", $sql));
        $this->execQuery($sql);
    }

    public function write(CsvFile $csv, array $table)
    {
        $preprocessor = new Preprocessor($csv);
        $filename = $preprocessor->process();

        $this->logger->info("BCP import started");
        $dstTableName = $table['dbName'];

        // create staging table
        $stagingTableName = $this->prefixTableName(uniqid('stage_') . '_', $dstTableName);
        $this->drop($stagingTableName);
        $table['dbName'] = $stagingTableName;
        $this->bcpCreateStage($table);
        $this->logger->info("BCP staging table created");

        // insert into staging
        $this->logger->info("BCP importing to staging table");
        $bcp = new BCP($this->db, $this->dbParams, $this->logger);
        $bcp->import($filename, $table);

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
            'INSERT INTO %s SELECT %s FROM %s',
            $this->escape($dstTableName),
            implode(',', $columns),
            $stagingTableName
        );
        $this->execQuery($query);
        $this->logger->info("BCP data moved to destination table");

        // drop staging
        $this->drop($stagingTableName);
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
        $sql = sprintf(
            "create table %s (",
            $table['incremental'] ? $table['dbName'] : $this->escape($table['dbName'])
        );

        $columns = $table['items'];
        foreach ($columns as $k => $col) {
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

            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ', ';
        }

        $sql = substr($sql, 0, -1);

        if (!empty($table['primaryKey'])) {
            $constraintId = uniqid(sprintf(
                "PK_%s_%s",
                str_replace('.', '_', $table['dbName']),
                implode('_', $table['primaryKey'])
            ));
            $sql .= PHP_EOL . sprintf(
                "CONSTRAINT %s PRIMARY KEY CLUSTERED (%s)",
                $constraintId,
                implode(',', $table['primaryKey'])
            ) . PHP_EOL;
        }

        $sql .= ")" . PHP_EOL;

        $this->logger->info(sprintf("Executing query: '%s'", $sql));

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
        $this->logger->debug(sprintf("Executing query: '%s'", $query));

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
        throw new \Exception("Not implemented");
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

    public function generateTmpName($table)
    {
        return $this->prefixTableName('bcp_tmp_', $table['dbName']);
    }

    private function prefixTableName($prefix, $tableName)
    {
        $tableNameArr = explode('.', $tableName);
        if (count($tableNameArr) > 1) {
            return $tableNameArr[0] . "." . $prefix . $tableNameArr[1];
        }

        return $prefix . $tableName;
    }
}
