<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Application;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Yaml\Yaml;

class MSSQLTest extends BaseTest
{
    const DRIVER = 'mssql';

    /** @var MSSQL */
    private $writer;

    private $config;

    public function setUp()
    {
        $this->config = $this->getConfig(self::DRIVER);
        $this->config['parameters']['writer_class'] = 'MSSQL';

        // create test database
        $dbParams = $this->config['parameters']['db'];
        $dsn = sprintf("dblib:host=%s;instance=MSSQLSERVER;charset=UTF-8", $dbParams['host']);
        $conn = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $conn->exec("USE master");
        $conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $dbParams['database'], $dbParams['database']));
        $conn->exec(sprintf("CREATE DATABASE %s", $dbParams['database']));
        $conn->exec(sprintf("USE %s", $dbParams['database']));

        $this->writer = $this->getWriter($this->config['parameters']);
        $tables = $this->config['parameters']['tables'];
        $conn = $this->writer->getConnection();

        foreach ($tables as $table) {
            $conn->exec(sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", $table['dbName'], $table['dbName']));
        }
    }

    public function testDrop()
    {
        $conn = $this->writer->getConnection();
        $conn->exec("CREATE TABLE dbo.dropMe (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

        $this->writer->drop("dbo.dropMe");

        $stmt = $conn->query("SELECT Distinct TABLE_NAME FROM information_schema.TABLES");
        $res = $stmt->fetchAll();

        $tableExists = false;
        foreach ($res as $r) {
            if ($r[0] == "dropMe") {
                $tableExists = true;
                break;
            }
        }

        $this->assertFalse($tableExists);
    }

    public function testCreate()
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $this->writer->create($table);
        }

        /** @var \PDO $conn */
        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT Distinct TABLE_NAME FROM information_schema.TABLES");
        $res = $stmt->fetchAll();

        $tableExits = false;
        foreach ($res as $r) {
            if ($r['TABLE_NAME'] == $tables[0]['dbName']) {
                $tableExits = true;
                break;
            }
        }

        $this->assertTrue($tableExits);
    }

    public function testWriteMssql()
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . "/" . $sourceTableId . ".csv";

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // table with special chars
        $table = $tables[1];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . "/" . $sourceTableId . ".csv";

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp-2');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["col1","col2"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);
    }

    public function testGetAllowedTypes()
    {
        $allowedTypes = $this->writer->getAllowedTypes();

        $this->assertEquals([
            'int', 'smallint', 'bigint', 'money',
            'decimal', 'real', 'float',
            'date', 'datetime', 'datetime2', 'smalldatetime', 'time', 'timestamp',
            'char', 'varchar', 'text',
            'nchar', 'nvarchar', 'ntext',
            'binary', 'varbinary', 'image',
        ], $allowedTypes);
    }

    public function testSingleLineInsert()
    {
        $rootPath = __DIR__ . '/../../../../';

        $config = Yaml::parse(file_get_contents($rootPath . 'tests/data/singleLine/config.yml'));
        $config['parameters']['writer_class'] = 'MSSQL';
        $config['parameters']['data_dir'] = $rootPath . 'tests/data/singleLine';

        $writer = $this->getWriter($this->config['parameters']);
        $conn = $writer->getConnection();

        $tables = $config['parameters']['tables'];

        foreach ($tables as $table) {
            $conn->exec(sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", $table['dbName'], $table['dbName']));
        }

        $application = new Application($config, new Logger());
        $application->run();

        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $config['parameters']['data_dir'] . "/in/tables/" . $sourceTableId . ".csv";

        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);
    }

    public function testUpsert()
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        $table = $tables[0];
        $sourceFilename = $this->dataDir . "/" . $table['tableId'] . ".csv";
        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $this->writer->create($targetTable);
        $this->writer->write(new CsvFile($sourceFilename), $targetTable);

        // second write
        $sourceFilename = $this->dataDir . "/" . $table['tableId'] . "_increment.csv";
        $this->writer->create($table);
        $this->writer->write(new CsvFile($sourceFilename), $table);
        $this->writer->upsert($table, $targetTable['dbName']);

        $stmt = $conn->query("SELECT * FROM {$targetTable['dbName']}");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->dataDir . "/" . $table['tableId'] . "_merged.csv";

        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testDisableEnableIndices()
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        $table = $tables[0];
        $sourceFilename = $this->dataDir . "/" . $table['tableId'] . ".csv";

        // first write
        $this->writer->create($table);
        $this->writer->write(new CsvFile($sourceFilename), $table);

        // create index
        $this->writer->getConnection()->exec(sprintf("
            CREATE NONCLUSTERED INDEX nameIndex 
            ON %s (%s)
        ", $table['dbName'], 'name'));

        $this->writer->getConnection()->exec(sprintf("
            CREATE NONCLUSTERED INDEX glassesIndex 
            ON %s (%s)
        ", $table['dbName'], 'glasses'));

        // disable
        $this->writer->modifyIndices($table['dbName'], 'disable');

        $stmt = $this->writer->getConnection()->query(sprintf("
            select I.name, I.is_disabled 
            from sys.indexes I
            inner join sys.tables T on I.object_id = T.object_id
            where I.type_desc = 'NONCLUSTERED' and T.name = '%s'
            and I.name is not null
        ", $table['dbName']));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertTrue(boolval($res[0]['is_disabled']));
        $this->assertTrue(boolval($res[1]['is_disabled']));

        // enable
        $this->writer->modifyIndices($table['dbName'], 'rebuild');
        $stmt = $conn->query(sprintf("
            select I.name, I.is_disabled 
            from sys.indexes I
            inner join sys.tables T on I.object_id = T.object_id
            where I.type_desc = 'NONCLUSTERED' and T.name = '%s'
            and I.name is not null
        ", $table['dbName']));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertFalse(boolval($res[0]['is_disabled']));
        $this->assertFalse(boolval($res[1]['is_disabled']));
    }

    public function testGetTableInfo()
    {
        $tables = $this->config['parameters']['tables'];
        $table = $tables[0];

        $this->writer->create($table);
        $tableInfo = $this->writer->getTableInfo($table['dbName']);
        $columns = $tableInfo['columns'];
        $expectedColumns = [
            [
                'name' => 'id',
                'type' =>  'int'
            ],
            [
                'name' => 'name',
                'type' =>  'nvarchar'
            ],
            [
                'name' => 'glasses',
                'type' =>  'nvarchar'
            ]
        ];

        $this->assertNotEmpty($columns);
        foreach ($columns as $key => $column) {
            $this->assertEquals($expectedColumns[$key]['name'], $column['COLUMN_NAME']);
            $this->assertEquals($expectedColumns[$key]['type'], $column['DATA_TYPE']);
        }
    }

    public function testCheckTargetTable()
    {
        $tables = $this->config['parameters']['tables'];
        $table = $tables[0];
        $this->writer->create($table);
        $this->writer->checkTargetTable($table);
    }

    public function testCheckTargetTableColumnNotFound()
    {
        $this->setExpectedException(
            'Keboola\DbWriter\Exception\UserException',
            "Column 'age' not found in destination table 'simple'"
        );
        $tables = $this->config['parameters']['tables'];
        $table = $tables[0];
        $this->writer->create($table);
        $table['items'][] = [
            'name' => 'age',
            'dbName' => 'age',
            'type' => 'int'
        ];
        $this->writer->checkTargetTable($table);
    }

    public function testCheckTargetTableDataTypeMismatch()
    {
        $this->setExpectedException(
            'Keboola\DbWriter\Exception\UserException',
            "Data type mismatch. Column 'glasses' is of type 'int' in writer, but is 'nvarchar' in destination table 'simple'"
        );
        $tables = $this->config['parameters']['tables'];
        $table = $tables[0];
        $this->writer->create($table);
        $table['items'][2]['type'] = 'int';
        $this->writer->checkTargetTable($table);
    }
}
