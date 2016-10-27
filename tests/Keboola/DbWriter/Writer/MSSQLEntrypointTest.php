<?php
namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class MSSQLEntrypointTest extends BaseTest
{
    const DRIVER = 'mssql';

    /** @var MSSQL */
    private $writer;

    private $config;

    public function testRunAction()
    {
        $this->config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runAction/config.yml'));
        $this->config['parameters']['writer_class'] = 'MSSQL';

        $this->writer = $this->getWriter($this->config['parameters']);

        // cleanup
        foreach ($this->config['parameters']['tables'] as $table) {
            $this->writer->drop($table['dbName']);
        }

        // run entrypoint
        $lastOutput = exec('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/runAction 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);
    }

    public function testRunActionIncremental()
    {
        $this->config = Yaml::parse(file_get_contents(ROOT_PATH . 'tests/data/runActionIncremental/config.yml'));
        $this->config['parameters']['writer_class'] = 'MSSQL';

        $this->writer = $this->getWriter($this->config['parameters']);

        // cleanup
        foreach ($this->config['parameters']['tables'] as $table) {
            $this->writer->drop($table['dbName']);
        }

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . ROOT_PATH . 'tests/data/runActionIncremental 2>&1');
        $process->run();

        $stmt = $this->writer->getConnection()->query("SELECT * FROM [simple]");
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
