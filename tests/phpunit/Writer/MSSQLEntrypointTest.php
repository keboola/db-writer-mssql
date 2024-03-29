<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;

class MSSQLEntrypointTest extends BaseTest
{
    private const BASIC_USER_LOGIN = 'basicUser';
    private const BASIC_USER_PASSWORD = 'Abcdefg1234';
    private const NO_PERM_USER_LOGIN = 'noPerm';
    private const NO_PERM_USER_PASSWORD = 'pwd12334$%^&';

    /** @var \PDO */
    private $conn;

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
        $this->conn = new \PDO(
            sprintf('sqlsrv:Server=%s', $dbParams['host']),
            $dbParams['user'],
            $dbParams['#password']
        );
        $this->conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // Drop database
        $this->conn->exec('USE master');
        $this->conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s')
            ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        ", $dbParams['database'], $dbParams['database']));
        $this->conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $dbParams['database'], $dbParams['database']));

        // Create database
        $this->conn->exec(sprintf('CREATE DATABASE %s COLLATE CZECH_CI_AS', $dbParams['database']));
        $this->conn->exec(sprintf('USE %s', $dbParams['database']));

        // Drop users
        $this->conn->exec(sprintf('DROP USER IF EXISTS %s', self::BASIC_USER_LOGIN));
        $this->conn->exec(sprintf("
            IF  EXISTS (SELECT * FROM sys.syslogins WHERE name = N'%s')
            DROP LOGIN %s
        ", self::BASIC_USER_LOGIN, self::BASIC_USER_LOGIN));
        $this->conn->exec(sprintf('DROP USER IF EXISTS %s', self::NO_PERM_USER_LOGIN));
        $this->conn->exec(sprintf("
            IF  EXISTS (SELECT * FROM sys.syslogins WHERE name = N'%s')
            DROP LOGIN %s
        ", self::NO_PERM_USER_LOGIN, self::NO_PERM_USER_LOGIN));

        // Create login and users
        $this->conn->exec(sprintf(
            "CREATE LOGIN %s WITH PASSWORD = '%s'",
            self::BASIC_USER_LOGIN,
            self::BASIC_USER_PASSWORD
        ));
        $this->conn->exec(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            self::BASIC_USER_LOGIN,
            self::BASIC_USER_LOGIN
        ));
        $this->conn->exec(sprintf(
            'GRANT CONTROL ON DATABASE::%s TO %s',
            $dbParams['database'],
            self::BASIC_USER_LOGIN
        ));
        $this->conn->exec(sprintf(
            'GRANT CONTROL ON SCHEMA::%s TO %s',
            'dbo',
            self::BASIC_USER_LOGIN
        ));
        $this->conn->exec(sprintf(
            'REVOKE EXECUTE TO %s',
            self::BASIC_USER_LOGIN
        ));
        $this->conn->exec(sprintf(
            "CREATE LOGIN %s WITH PASSWORD = '%s'",
            self::NO_PERM_USER_LOGIN,
            self::NO_PERM_USER_PASSWORD
        ));
        $this->conn->exec(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            self::NO_PERM_USER_LOGIN,
            self::NO_PERM_USER_LOGIN
        ));
        $this->conn->exec(sprintf(
            'REVOKE EXECUTE TO %s',
            self::NO_PERM_USER_LOGIN
        ));

        $this->conn->exec('USE test');

        $this->cleanup($config);
    }

    public function testRunFull(): void
    {
        $config = $this->initInputFiles('runFull');
        $process = $this->runApp();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        $expectedFilename = $this->testsDataPath . '/runFull/expected/simple.csv';
        $resFilename = $this->writeCsvFromDB($config['parameters']['db'], 'simple-with_special-chars-in_name');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $expectedFilename = $this->testsDataPath . '/runFull/expected/special.csv';
        $resFilename = $this->writeCsvFromDB($config['parameters']['db'], 'special');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query('SELECT * FROM [nullable] WHERE id=1');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertNull($res[0]['nullable']);

        $stmt = $writer->getConnection()->query('SELECT * FROM [nullable] WHERE id=0');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertEquals('not null', $res[0]['nullable']);

        // check data types and keys
        $query = 'SELECT
                c.name \'column_name\',
                t.Name \'data_type\',
                c.max_length \'max_length\',
                c.precision ,
                c.scale ,
                c.is_nullable,
                cast(CASE WHEN c.default_object_id > 0 THEN 1 ELSE 0 END as bit) as has_default,
                ISNULL(i.is_primary_key, 0) \'primary_key\'
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            WHERE c.object_id = OBJECT_ID(\'simple-with_special-chars-in_name\')';
        $stmt = $writer->getConnection()->query($query);
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $this->assertEquals('id', $res[0]['column_name']);
        $this->assertEquals('int', $res[0]['data_type']);
        $this->assertEquals(1, $res[0]['primary_key']);
        $this->assertEquals(0, $res[0]['has_default']);

        $this->assertEquals('name', $res[1]['column_name']);
        $this->assertEquals('nvarchar', $res[1]['data_type']);
        $this->assertEquals(0, $res[1]['primary_key']);
        $this->assertEquals(1, $res[1]['has_default']);

        $this->assertEquals('glasses', $res[2]['column_name']);
        $this->assertEquals('nvarchar', $res[2]['data_type']);
        $this->assertEquals(0, $res[2]['primary_key']);
        $this->assertEquals(0, $res[2]['has_default']);
    }

    public function testRunRow(): void
    {
        $config = $this->initInputFiles('runRow');
        $process = $this->runApp();
        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());

        $expectedFilename = $this->testsDataPath . '/runFull/expected/simple.csv';
        $resFilename = $this->writeCsvFromDB($config['parameters']['db'], 'simple');
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

        $expectedFilename = $this->testsDataPath . '/runFull/expected/simple.csv';
        $resFilename = $this->writeCsvFromDB($config['parameters']['db'], 'simple-with_special-chars-in_name');
        $this->assertFileEquals($expectedFilename, $resFilename);

        $expectedFilename = $this->testsDataPath . '/runFull/expected/special.csv';
        $resFilename = $this->writeCsvFromDB($config['parameters']['db'], 'special');
        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testBcpRetry(): void
    {
        $config = $this->initConfig('runRetryBcp');

        $this->initInputFiles('runRetryBcp', $config);

        $process = $this->runApp();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString('Retrying... [4x]', $process->getOutput());

        $this->assertEquals(5, substr_count($process->getOutput(), 'BCP staging table created'));
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

        $expectedFilename = $this->testsDataPath . '/runIncremental/expected/simple_merged.csv';
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

        $expectedFilename = $this->testsDataPath . '/runIncremental/expected/simple_merged.csv';
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

            $config['storage']['input']['tables'] = [
                [
                    'source' => 'text',
                    'destination' => 'text.csv',
                ],
            ];

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

        $expectedFilename = $this->testsDataPath . '/runFull/expected/text.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testBinaryDataTypes(): void
    {
        $config = $this->initConfig('runBinary');
        $this->initInputFiles('runBinary', $config);

        $process = $this->runApp();
        $this->assertEquals(0, $process->getExitCode(), $process->getErrorOutput() . $process->getOutput());

        $writer = $this->getWriter($config['parameters']);
        /** @var \PDOStatement $stmt */
        $stmt = $writer->getConnection()->query('SELECT * FROM [binary]');
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $binaryCols = ['image','binary','varbinary', 'image_null','binary_null','varbinary_null'];
        $csv = new CsvFile($resFilename);
        $header = null;
        foreach ($res as $row) {
            if (!$header) {
                $header = array_keys($row);
                $csv->writeRow($header);
            }

            // Convert binary columns to readable format
            foreach ($binaryCols as $name) {
                if (array_key_exists($name, $row)) {
                    $value = $row[$name];
                    if ($value === null) {
                        $row[$name] = '(null)';
                    } elseif ($value === '') {
                        $row[$name] = '';
                    } else {
                        $row[$name] = '0x' . bin2hex(trim($row[$name], "\0"));
                    }
                }
            }

            $csv->writeRow($row);
        }

        // MsSQL cannot convert hex values with odd length -> converted as string
        $oddHex = '0x' . bin2hex("0\0x\0a\0b\0c\0d\0e");
        // String doesn't start with 0x -> converted as string
        $dog = '0x' . bin2hex("d\0o\0g");
        $expected = <<<EOT
"id","image","binary","varbinary","image_null","binary_null","varbinary_null"
"1","$oddHex","0xabcdef1111","0xabcdef2222","0xabcdef3333","0xabcdef4444","0xabcdef5555"
"2","0xabcdef1234","0xabcdef3456","$dog","0xabcdef4567","0xabcdef5678","0xabcdef9012"
"3","","0x","","(null)","(null)","(null)"

EOT;

        $this->assertSame($expected, file_get_contents($resFilename));
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

    public function testInputMappingMissing(): void
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

        $this->assertContains(
            'Table "nonExistent" in storage input mapping cannot be found.',
            $process->getErrorOutput()
        );
    }

    public function testExceptionLogging(): void
    {
        $config = $this->initConfig('runFull', function ($config) {
            $config['parameters']['tables'][0] = [
                'tableId' => 'nonExistent',
                'dbName' => 'asdfg',
            ];
            unset($config['storage']);
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

    public function testRetry(): void
    {
        $testDataSet = 'runIncremental';
        $config = $this->initConfig($testDataSet);

        // Modify config
        unset($config['parameters']['tables'][0]);
        unset($config['parameters']['tables'][2]);

        $orgConfig = $config;
        $table = $orgConfig['parameters']['tables'][1];

        $config['parameters']['db']['user'] = self::NO_PERM_USER_LOGIN;
        $config['parameters']['db']['#password'] = self::NO_PERM_USER_PASSWORD;
        $this->initInputFiles($testDataSet, $config);

        // Create table
        $writer = $this->getWriter($orgConfig['parameters']);
        $writer->create($table);

        // Insert to table is denied
        $this->conn->exec(sprintf('GRANT CONTROL ON DATABASE::test TO %s', self::NO_PERM_USER_LOGIN));
        $this->conn->exec(sprintf('GRANT CONTROL ON SCHEMA::dbo TO %s', self::NO_PERM_USER_LOGIN));
        $this->conn->exec(sprintf('DENY INSERT ON OBJECT::dbo.simple TO %s', self::NO_PERM_USER_LOGIN));

        // Run app
        $process = $this->runApp();

        // Check retry in output
        $output = $process->getOutput();
        $errorOutput = $process->getErrorOutput();
        $expectedError =
            'The INSERT permission was denied ' .
            'on the object \'simple\', database \'test\', schema \'dbo\'.';
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertStringContainsString($expectedError . '. Retrying... [1x]', $output);
        $this->assertStringContainsString($expectedError . '. Retrying... [2x]', $output);
        $this->assertStringContainsString($expectedError . '. Retrying... [3x]', $output);
        $this->assertStringContainsString($expectedError . '. Retrying... [4x]', $output);
        $this->assertStringContainsString($expectedError, $errorOutput);

        // Check table "simple" was not removed in retry
        $this->assertNotEmpty(
            $this->conn
                ->query("SELECT * FROM test.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'simple'")
                ->fetchAll()
        );
    }

    public function testRunEmptyConfig(): void
    {
        $config = [
            'action' => 'run',
            'parameters' => [
                'data_dir' => $this->dataDir,
                'writer_class' => 'MSSQL',
                'db' => [
                    'driver' => 'mssql',
                    'host' => 'mssql',
                    'port' => 1433,
                    'database' =>'test',
                    'user' =>'sa',
                    '#password' =>'yourStrong(!)Password',
                ],
            ],
        ];
        file_put_contents($this->tmpDataPath . '/config.json', json_encode($config));

        $process = $this->runApp();
        $this->assertEquals(1, $process->getExitCode(), $process->getOutput());
        $this->assertSame(
            "The child node \"tableId\" at path \"parameters\" must be configured.\n",
            $process->getErrorOutput()
        );
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

    private function writeCsvFromDB(array $db, string $table): string
    {
        $host = $db['host'];
        $host .= (isset($dbParams['port']) && $dbParams['port'] !== '1433') ? ',' . $dbParams['port'] : '';
        $host .= empty($dbParams['instance']) ? '' : '\\\\' . $dbParams['instance'];
        $options = [];
        $options[] = 'Server=' . $host;
        $options[] = 'Database=' . $db['database'];
        $dsn = sprintf('sqlsrv:%s', implode(';', $options));
        $pdo = new \PDO($dsn, $db['user'], $db['#password']);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $stmt = $pdo->query(sprintf('SELECT * FROM [%s]', $table));
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);

        // Dump columns names
        $columns = [];
        for ($i=0; $i<$stmt->columnCount(); $i++) {
            $meta = $stmt->getColumnMeta($i);
            $columns[] = $meta['name'];
        }
        $csv->writeRow($columns);

        // Dump rows
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
