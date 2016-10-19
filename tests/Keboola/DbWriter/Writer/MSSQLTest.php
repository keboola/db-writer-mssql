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
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'wr-db-mssql');
        }

        $this->config = $this->getConfig(self::DRIVER);
        $this->config['parameters']['writer_class'] = 'MSSQL';
        $this->writer = $this->getWriter($this->config['parameters']);
        $conn = $this->writer->getConnection();

        $tables = $this->config['parameters']['tables'];

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
            if ('dbo.' . $r['TABLE_NAME'] == $tables[0]['dbName']) {
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

        // ignored columns
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . "/" . $sourceTableId . ".csv";

        $table['items'][2]['type'] = 'IGNORE';

        $this->writer->drop($outputTableName);
        $this->writer->create($table);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resArr = [];
        foreach ($res as $row) {
            $resArr[] = array_values($row);
        }

        $srcArr = [];
        $csv = new CsvFile($sourceFilename);
        $csv->next();
        $csv->next();

        while ($csv->current()) {
            $currRow = $csv->current();
            unset($currRow[2]);
            $srcArr[] = array_values($currRow);
            $csv->next();
        }

        $this->assertEquals($srcArr, $resArr);
    }

    public function testGetAllowedTypes()
    {
        $allowedTypes = $this->writer->getAllowedTypes();

        $this->assertEquals([
            'int', 'smallint', 'bigint', 'money',
            'decimal', 'real', 'float',
            'date', 'datetime', 'datetime2', 'time', 'timestamp',
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

    public function testReorderRenameIgnoreColumns()
    {
        $conn = $this->writer->getConnection();
        $tables = $this->config['parameters']['tables'];

        $table = $tables[0];

        // reorder
        $table['items'] = array_reverse($table['items']);

        // rename
        foreach ($table['items'] AS $key => $column) {
            $table['items'][$key]['dbName'] = md5($column['dbName']);
        }
        foreach ($table['primaryKey'] AS $key => $column) {
            $table['primaryKey'][$key] = md5($column);
        }

        // ignore
        foreach ($table['items'] AS $key => $column) {
            if ($column['name'] === 'glasses') {
                $table['items'][$key]['type'] = 'IGNORE';
            }
        }

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


        $expectedFilename = $this->dataDir . "/" . $table['tableId'] . "_merged.csv";
        $expectedCsv = new CsvFile($expectedFilename);

        // prepare validation file
        $expectedHeaderMap = array();
        foreach ($table['items'] AS $column) {
            if ($column['type'] === 'IGNORE') {
                continue;
            }

            $expectedHeaderMap[$column['name']] = $column['dbName'];
        }

        $tmpExpectedFilename = tempnam('/tmp', md5($expectedFilename));
        $tmpExpectedCsv = new CsvFile($tmpExpectedFilename);

        $header = $expectedCsv->getHeader();
        foreach ($expectedCsv AS $i => $row) {
            if (!$i) {
                $tmpExpectedCsv->writeRow($expectedHeaderMap);
                continue;
            }

            $newRow = [];

            $row = array_combine($header, $row);
            foreach ($expectedHeaderMap AS $originName => $newName) {
                $newRow[$newName] = $row[$originName];
            }

            $tmpExpectedCsv->writeRow($newRow);
        }

        $stmt = $conn->query("SELECT " . implode(', ', $expectedHeaderMap) . " FROM {$targetTable['dbName']}");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow($expectedHeaderMap);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($tmpExpectedFilename, $resFilename);
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

//    public function testExecutor()
//    {
//        $config = $this->getConfig(self::DRIVER);
//        $tables = $config['parameters']['tables'];
//        $outputTableName = $tables[0]['dbName'];
//        $sourceTableId = $tables[0]['tableId'];
//        $sourceFilename = $this->dataDir . "/" . self::DRIVER . "/in/tables/" . $sourceTableId . ".csv";
//
//        $executor = $this->getExecutor(self::DRIVER);
//        $executor->run();
//
//        $conn = $this->getWriter(self::DRIVER)->getConnection();
//        $stmt = $conn->query("SELECT * FROM $outputTableName");
//        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
//
//        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
//        $csv = new CsvFile($resFilename);
//        $csv->writeRow(["id","name","hasGlasses","double"]);
//        foreach ($res as $row) {
//            $csv->writeRow($row);
//        }
//
//        $this->assertFileEquals($sourceFilename, $resFilename);
//    }
}
