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

    public function testRunAction()
    {
        // cleanup
        $config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runAction/config.yml'));
        $config['parameters']['writer_class'] = 'MSSQL';
        $writer = $this->getWriter($config['parameters']);
        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/runAction');
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
    }

    public function testRunActionIncremental()
    {
        $config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runActionIncremental/config_default.yml'));
        $config['parameters']['writer_class'] = 'MSSQL';
        $writer = $this->getWriter($config['parameters']);
        foreach ($config['parameters']['tables'] as $table) {
            $writer->drop($table['dbName']);
        }

        $tables = $config['parameters']['tables'];
        $table = $tables[0];

        // reorder
        $table['items'] = array_reverse($table['items']);

        $tables[0] = $table;
        $config['parameters']['tables'] = $tables;

        file_put_contents($this->tmpDataPath . '/runActionIncremental/config.yml', Yaml::dump($config));

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpDataPath . '/runActionIncremental 2>&1');
        $process->run();

        $stmt = $writer->getConnection()->query("SELECT * FROM simple");
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->tmpDataPath . '/runActionIncremental/simple_merged.csv';

        $this->assertFileEquals($expectedFilename, $resFilename);

        $this->assertEquals(0, $process->getExitCode());
    }

    public function testConnectionAction()
    {
        $rootPath = __DIR__ . '/../../../../';

        $lastOutput = exec('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/connectionAction 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);

        $this->assertCount(1, $output);
        $this->assertEquals($lastOutput, reset($output));

        $data = json_decode($lastOutput, true);
        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }
}
