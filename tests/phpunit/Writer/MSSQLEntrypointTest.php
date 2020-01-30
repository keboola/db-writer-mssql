<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;

class MSSQLEntrypointTest extends BaseTest
{
    /** @var string */
    private $rootPath = __DIR__ . '/../../../';

    /** @var string */
    private $testsDataPath = __DIR__ . '/../../data';

    /** @var string */
    private $tmpDataPath = '/tmp/wr-db-mssql/data';

    public function setUp(): void
    {
        $config= $this->getConfig();
        $config['parameters']['writer_class'] = 'MSSQL';

        // create test database
        $dbParams = $config['parameters']['db'];
        $conn = new \PDO(sprintf('sqlsrv:Server=%s', $dbParams['host']), $dbParams['user'], $dbParams['#password']);
        $conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $conn->exec('USE master');
        $conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $dbParams['database'], $dbParams['database']));
        $conn->exec(sprintf('CREATE DATABASE %s COLLATE CZECH_CI_AS', $dbParams['database']));
        $conn->exec(sprintf('USE %s', $dbParams['database']));
        $conn->exec(sprintf('DROP USER IF EXISTS %s', 'basicUser'));
        $conn->exec(sprintf("
            IF  EXISTS (SELECT * FROM sys.syslogins WHERE name = N'%s')
            DROP LOGIN %s
        ", 'basicUser', 'basicUser'));
        $conn->exec(sprintf("CREATE LOGIN %s WITH PASSWORD = '%s'", 'basicUser', 'Abcdefg1234'));
        $conn->exec(sprintf('CREATE USER %s FOR LOGIN %s', 'basicUser', 'basicUser'));
        $conn->exec(sprintf('GRANT CONTROL ON DATABASE::%s TO %s', $dbParams['database'], 'basicUser'));
        $conn->exec(sprintf('GRANT CONTROL ON SCHEMA::%s TO %s', 'dbo', 'basicUser'));
        $conn->exec(sprintf('REVOKE EXECUTE TO %s', 'basicUser'));

        $this->cleanup($config);
    }

    public function testRunFull(): void
    {
        $config = $this->initInputFiles('runFull');
        $process = $this->runApp();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/simple.csv';
        $resFilename = $this->writeCsvFromDB($config, 'simple');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/special.csv';
        $resFilename = $this->writeCsvFromDB($config, 'special');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query('SELECT * FROM [nullable] WHERE id=1');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertNull($res[0]['nullable']);

        $stmt = $writer->getConnection()->query('SELECT * FROM [nullable] WHERE id=0');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertEquals('not null', $res[0]['nullable']);
    }

    public function testRunRow(): void
    {
        $config = $this->initInputFiles('runRow');
        $process = $this->runApp();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/simple.csv';
        $resFilename = $this->writeCsvFromDB($config, 'simple');
        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testInvalidRunRow(): void
    {
        $config = $this->initConfig('runRow');
        unset($config['parameters']['tableId']);
        $this->initInputFiles('runRow', $config);
        $process = $this->runApp();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertEquals(
            "The child node \"tableId\" at path \"parameters\" must be configured.\n",
            $process->getErrorOutput()
        );
    }

    public function testRunBasicUser(): void
    {
        $config = $this->initConfig('runFull', function ($config) {
            $config['parameters']['db'] = array_merge(
                $config['parameters']['db'],
                [
                    'user' => 'basicUser',
                    'password' => 'Abcdefg1234',
                    '#password' => 'Abcdefg1234',
                    'collation' => 'CZECH_CI_AS',
                ]
            );

            return $config;
        });

        $this->initInputFiles('runFull', $config);

        $process = $this->runApp();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/simple.csv';
        $resFilename = $this->writeCsvFromDB($config, 'simple');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/special.csv';
        $resFilename = $this->writeCsvFromDB($config, 'special');
        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testRunIncremental(): void
    {
        $config = $this->initConfig('runIncremental', function ($config) {
            // shuffle columns in one table
            $table = $config['parameters']['tables'][0];
            $table['items'] = array_reverse($table['items']);
            $config['parameters']['tables'][0] = $table;

            return $config;
        });

        $this->initInputFiles('runIncremental', $config);

        $process = $this->runApp();

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query('SELECT * FROM simple');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id', 'name', 'glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->testsDataPath . '/runIncremental/simple_merged.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testIncrementalWithIndex(): void
    {
        $config = $this->initInputFiles('runIncremental');

        // create table with index
        $table = $config['parameters']['tables'][0];
        $writer = $this->getWriter($config['parameters']);
        $writer->create($table);
        $writer->getConnection()->exec(sprintf('
            CREATE NONCLUSTERED INDEX someIndexNameId 
            ON %s (%s)
        ', $table['dbName'], 'name'));

        // run
        $process = $this->runApp();

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query('SELECT * FROM simple');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id', 'name', 'glasses']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->testsDataPath . '/runIncremental/simple_merged.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testTextDataTypes(): void
    {
        $config = $this->initConfig('runFull', function ($config) {
            $config['parameters']['tables'] = [[
                'tableId' => 'text',
                'dbName' => 'text',
                'export' => true,
                'incremental' => false,
                'primaryKey' => ['id'],
                'items' => [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'int',
                        'size' => null,
                        'nullable' => null,
                        'default' => null,
                    ],
                    [
                        'name' => 'text',
                        'dbName' => 'text',
                        'type' => 'text',
                        'size' => null,
                        'nullable' => null,
                        'default' => null,
                    ],
                    [
                        'name' => 'ntext',
                        'dbName' => 'ntext',
                        'type' => 'ntext',
                        'size' => null,
                        'nullable' => null,
                        'default' => null,
                    ],
                ],
            ]];

            return $config;
        });
        $this->initInputFiles('runFull', $config);

        $process = $this->runApp();

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query('SELECT * FROM [text]');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id', 'text', 'ntext']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->testsDataPath . '/runFull/in/tables/text.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testConnectionAction(): void
    {
        $this->initInputFiles('testConnection');
        $process = $this->runApp();

        $this->assertEquals(0, $process->getExitCode());
        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testRowConnectionAction(): void
    {
        $this->initInputFiles('testRowConnection');
        $process = $this->runApp();

        $this->assertEquals(0, $process->getExitCode());
        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testExceptionLogging(): void
    {
        $config = $this->initConfig('runFull', function ($config) {
            $config['parameters']['tables'][0] = [
                'tableId' => 'nonExistent',
                'dbName' => 'asdfg',
            ];
            return $config;
        });

        (new Process('rm -rf ' . $this->tmpDataPath . '/*'))->mustRun();
        mkdir($this->tmpDataPath . '/in/tables', 0777, true);
        file_put_contents($this->tmpDataPath . '/config.json', json_encode($config));

        $process = new Process(sprintf('php %s/run.php --data=%s', $this->rootPath, $this->tmpDataPath));
        $process->run();

        $this->assertContains('errFile', $process->getErrorOutput());
        $this->assertContains('errLine', $process->getErrorOutput());
        $this->assertContains('trace', $process->getErrorOutput());
        $this->assertContains('"class":"Keboola\\\\DbWriter\\\\Application"', $process->getErrorOutput());
    }

    private function initInputFiles(string $subDir, ?array $config = null): array
    {
        $config = $config ?: $this->initConfig($subDir);

        $this->cleanup($config);

        (new Process('rm -rf ' . $this->tmpDataPath . '/*'))->mustRun();
        mkdir($this->tmpDataPath . '/in/tables', 0777, true);
        file_put_contents($this->tmpDataPath . '/config.json', json_encode($config));

        if (isset($config['parameters']['tables'])) {
            foreach ($config['parameters']['tables'] as $table) {
                copy(
                    $this->testsDataPath . '/' . $subDir . '/in/tables/' . $table['tableId'] . '.csv',
                    $this->tmpDataPath . '/in/tables/' . $table['tableId'] . '.csv'
                );
            }
        } elseif (isset($config['parameters']['tableId'])) {
            copy(
                $this->testsDataPath . '/' . $subDir . '/in/tables/' . $config['parameters']['tableId'] . '.csv',
                $this->tmpDataPath . '/in/tables/' . $config['parameters']['tableId'] . '.csv'
            );
        }

        return $config;
    }

    private function writeCsvFromDB(array $config, string $tableId): string
    {
        $writer = $this->getWriter($config['parameters']);
        if (isset($config['parameters']['tables'])) {
            $tableArr = array_filter($config['parameters']['tables'], function ($item) use ($tableId) {
                return $item['tableId'] === $tableId;
            });
            $table = array_shift($tableArr);
        } else {
            $table = $config['parameters'];
        }

        $stmt = $writer->getConnection()->query(sprintf('SELECT * FROM [%s]', $table['dbName']));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(array_map(function ($item) {
            return $item['dbName'];
        }, $table['items']));
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        return $resFilename;
    }

    private function runApp(): Process
    {
        $process = new Process(sprintf('php %s/run.php --data=%s', $this->rootPath, $this->tmpDataPath));
        $process->setTimeout(300);
        $process->run();

        return $process;
    }

    private function cleanup(array $config): void
    {
        $writer = $this->getWriter($config['parameters']);
        $conn = $writer->getConnection();
        if (isset($config['parameters']['tables'])) {
            $tables = $config['parameters']['tables'];
            foreach ($tables as $table) {
                $conn->exec(
                    sprintf(
                        "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
                        $this->escape($table['dbName']),
                        $this->escape($table['dbName'])
                    )
                );
            }
        } elseif (isset($config['parameters']['dbName'])) {
            $conn->exec(
                sprintf(
                    "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
                    $config['parameters']['dbName'],
                    $config['parameters']['dbName']
                )
            );
        }
    }

    private function initConfig(?string $subDir, ?callable $modify = null): array
    {
        $config = json_decode(file_get_contents($this->testsDataPath . '/' . $subDir . '/config.json'), true);

        return ($modify !== null) ? $modify($config) : $config;
    }

    private function escape(string $obj): string
    {
        $objNameArr = explode('.', $obj);
        if (count($objNameArr) > 1) {
            return $objNameArr[0] . '.[' . $objNameArr[1] . ']';
        }

        return '[' . $objNameArr[0] . ']';
    }
}
