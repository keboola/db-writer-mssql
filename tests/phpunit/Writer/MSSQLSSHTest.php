<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\DbWriter\Writer\MSSQL;
use Keboola\DbWriter\WriterFactory;
use Monolog\Handler\TestHandler;

class MSSQLSSHTest extends BaseTest
{
    /** @var MSSQL */
    private $writer;

    /** @var array */
    private $config;

    /** @var TestHandler */
    private $testHandler;

    /** @var string */
    protected $dataDir = __DIR__ . '/../../data';

    public function setUp(): void
    {
        $this->config = $this->getConfig();
        $this->config['parameters']['writer_class'] = 'MSSQL';
        $this->config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => '',
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'mssql',
            'remotePort' => '1433',
        ];
        // create test database
        $dbParams = $this->config['parameters']['db'];
        $conn = new \PDO(
            sprintf('sqlsrv:Server=%s', $dbParams['host']),
            $dbParams['user'],
            $dbParams['#password']
        );
        $conn->exec('USE master');
        $conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $dbParams['database'], $dbParams['database']));
        $conn->exec(sprintf('CREATE DATABASE %s', $dbParams['database']));
        $conn->exec(sprintf('USE %s', $dbParams['database']));

        $this->testHandler = new TestHandler();
        $logger = new Logger('wr-db-mssql-tests');
        $logger->setHandlers([$this->testHandler]);

        $writerFactory = new WriterFactory($this->config['parameters']);
        $this->writer = $writerFactory->create($logger);
    }

    public function testWriteMssql(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . '.csv';

        $this->writer->drop($outputTableName);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id','name','glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // table with special chars
        $table = $tables[1];
        $sourceTableId = $table['tableId'];
        $outputTableName = $table['dbName'];
        $sourceFilename = $this->dataDir . '/' . $sourceTableId . '.csv';

        $this->writer->drop($outputTableName);
        $this->writer->write(new CsvFile(realpath($sourceFilename)), $table);

        $conn = $this->writer->getConnection();
        $stmt = $conn->query("SELECT * FROM $outputTableName");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp-2');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['col1','col2']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($sourceFilename, $resFilename);

        // test log messages
        $records = $this->testHandler->getRecords();
        $records = array_filter($records, function ($record) {
            if ($record['level_name'] !== 'DEBUG') {
                return true;
            }

            return false;
        });

        $this->assertArrayHasKey('message', $records[0]);
        $this->assertArrayHasKey('level', $records[0]);
        $this->assertArrayHasKey('message', $records[1]);
        $this->assertArrayHasKey('level', $records[1]);

        $this->assertEquals(Logger::INFO, $records[0]['level']);
        $this->assertRegExp('/Creating SSH tunnel/ui', $records[0]['message']);

        $this->assertEquals(Logger::INFO, $records[1]['level']);
        $this->assertRegExp('/Connecting to DSN/ui', $records[1]['message']);
    }
}
