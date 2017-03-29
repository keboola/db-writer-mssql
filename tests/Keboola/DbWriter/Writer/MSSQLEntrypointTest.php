<?php
namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class MSSQLEntrypointTest extends BaseTest
{
    const DRIVER = 'mssql';

    private $tmpDataPath = '/tmp/wr-db-mssql/data';

    public function setUp()
    {
        $config= $this->getConfig(self::DRIVER);
        $config['parameters']['writer_class'] = 'MSSQL';

        // create test database
        $dbParams = $config['parameters']['db'];
        $dsn = sprintf("dblib:host=%s;charset=UTF-8", $dbParams['host']);
        $conn = new \PDO($dsn, $dbParams['user'], $dbParams['#password']);
        $conn->exec("USE master");
        $conn->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $dbParams['database'], $dbParams['database']));
        $conn->exec(sprintf("CREATE DATABASE %s", $dbParams['database']));
        $conn->exec(sprintf("USE %s", $dbParams['database']));
    }

    public function testRunAction()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/runAction');
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput());
    }

    public function testRunActionIncremental()
    {
        $config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runActionIncremental/config_default.yml'));
        $config['parameters']['writer_class'] = 'MSSQL';
        $tables = $config['parameters']['tables'];
        $table = $tables[0];
        $table['items'] = array_reverse($table['items']);
        $tables[0] = $table;
        $config['parameters']['tables'] = $tables;

        (new Process('rm -rf ' . $this->tmpDataPath . '/runActionIncremental'))->mustRun();
        mkdir($this->tmpDataPath . '/runActionIncremental/in/tables', 0777, true);
        file_put_contents($this->tmpDataPath . '/runActionIncremental/config.yml', Yaml::dump($config));

        foreach (['simple', 'simple_increment', 'simple_increment2',] as $fileName) {
            copy(
                ROOT_PATH . 'tests/data/runActionIncremental/in/tables/' . $fileName . '.csv',
                $this->tmpDataPath . '/runActionIncremental/in/tables/' . $fileName . '.csv'
            );
        }

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataPath . '/runActionIncremental 2>&1');
        $process->mustRun();

        $writer = $this->getWriter($config['parameters']);
        $stmt = $writer->getConnection()->query("SELECT * FROM simple");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = ROOT_PATH . 'tests/data/runActionIncremental/simple_merged.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);
        $this->assertEquals(0, $process->getExitCode());
    }

    public function testConnectionAction()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/connectionAction 2>&1');
        $process->mustRun();

        $this->assertEquals(0, $process->getExitCode());
        $data = json_decode($process->getOutput(), true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testRunBCP()
    {
        // cleanup
        $config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runBCP/config.yml'));
        $config['parameters']['writer_class'] = 'MSSQL';
        $writer = $this->getWriter($config['parameters']);
        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/runBCP');
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
    }
}
