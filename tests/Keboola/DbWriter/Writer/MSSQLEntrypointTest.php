<?php
namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Test\BaseTest;
use Symfony\Component\Yaml\Yaml;

class MSSQLEntrypointTest extends BaseTest
{
    const DRIVER = 'mssql';

    /** @var MSSQL */
    private $writer;

    private $config;

    public function testRunAction()
    {
        $rootPath = __DIR__ . '/../../../../';

        $this->config = Yaml::parse(file_get_contents($rootPath . 'tests/data/runAction/config.yml'));
        $this->config['parameters']['writer_class'] = 'MSSQL';

        $this->writer = $this->getWriter($this->config['parameters']);

        // cleanup
        foreach ($this->config['parameters']['tables'] AS $table) {
            $this->writer->drop($table['dbName']);
        }

        // run entrypoint
        $lastOutput = exec('php ' . $rootPath . 'run.php --data=' . $rootPath . 'tests/data/runAction 2>&1', $output, $returnCode);

        $this->assertEquals(0, $returnCode);

    }
}
