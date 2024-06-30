<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\FunctionalTests;

use Keboola\Csv\CsvWriter;
use Keboola\Csv\Exception;
use Keboola\Csv\InvalidArgumentException;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Mssql\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbWriter\Mssql\TraitTests\PrepareDatabase;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\MSSQLConnectionFactory;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DatadirTest extends AbstractDatadirTestCase
{
    use PrepareDatabase;
    use CloseSshTunnelsTrait;
    public MSSQLConnection $connection;
    protected string $testProjectDir;
    public ?string $orderResults = null;

    public function __construct(
        ?string $name = null,
        array $data = [],
        string $dataName = '',
    ) {
        putenv('SSH_PRIVATE_KEY=' . file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . file_get_contents('/root/.ssh/id_rsa.pub'));
        parent::__construct($name, $data, $dataName);
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->closeSshTunnels();
        $connectionFactory = new MSSQLConnectionFactory(new TestLogger());

        // prepare database needs master database
        $databaseConfig = $this->getDatabaseConfig('master');
        $this->prepareDatabase(
            $connectionFactory->create($databaseConfig),
            (string) getenv('DB_DATABASE'),
        );

        $databaseConfig = $this->getDatabaseConfig();
        $this->connection = $connectionFactory->create($databaseConfig);
        $this->orderResults = null;
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $setUpPhpFile = $this->testProjectDir . '/tearDown.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }
            $initCallback($this);
        }
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {
        $tempDatadir = $this->getTempDatadir($specification);

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->dumpTables($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTables(string $tmpFolder): void
    {
        $dumpDir = $tmpFolder . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir);

        foreach ($this->getTableNames() as $tableName) {
            $this->dumpTableData($tableName, $dumpDir);
        }
    }

    private function getTableNames(): array
    {
        /** @var array{'TABLE_NAME': string}|array $tables */
        $tables = $this->connection->fetchAll(
            sprintf(
                'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = \'%s\';',
                getenv('DB_DATABASE'),
            ),
        );
        $tables = array_filter($tables, fn($item) => !str_starts_with($item['TABLE_NAME'], 'tmp_'));

        return array_map(function ($item) {
            return $item['TABLE_NAME'];
        }, $tables);
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTableData(string $tableName, string $tmpFolder): void
    {
        $csvDumpFile = new CsvWriter(sprintf('%s/%s.csv', $tmpFolder, $tableName));
        $sql = sprintf('SELECT * FROM %s', $this->connection->quoteIdentifier($tableName));

        // get primary keys
        /** @var array{'COLUMN_NAME': string}|array $primaryKeysInDb */
        $primaryKeysInDb = $this->connection->fetchAll(sprintf(
            <<<SQL
select CCU.COLUMN_NAME
from INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
left join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU on TC.TABLE_NAME = CCU.TABLE_NAME
where TC.TABLE_NAME = %s AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY';
SQL,
            $this->connection->quote($tableName),
        ));

        $primaryKeysInDb = array_map(
            fn(array $item) => $this->connection->quoteIdentifier($item['COLUMN_NAME']),
            $primaryKeysInDb,
        );

        if (!empty($primaryKeysInDb)) {
            $sql .= sprintf(
                ' ORDER BY %s',
                implode(',', $primaryKeysInDb),
            );
        } elseif ($this->orderResults) {
            $sql .= sprintf(
                ' ORDER BY %s',
                $this->connection->quoteIdentifier($this->orderResults),
            );
        }

        $rows = $this->connection->fetchAll($sql);
        if ($rows) {
            $csvDumpFile->writeRow(array_keys(current($rows)));
            foreach ($rows as $row) {
                $row = array_map(fn($v) => is_null($v) ? '' : $v, $row);
                $csvDumpFile->writeRow($row);
            }
        }
    }

    public function getDatabaseConfig(?string $database = null): MSSQLDatabaseConfig
    {
        $config = [
            'host' =>  getenv('DB_HOST'),
            'port' => getenv('DB_PORT'),
            'database' => $database ?? getenv('DB_DATABASE'),
            'user' => getenv('DB_USER'),
            '#password' => getenv('DB_PASSWORD'),
        ];

        return MSSQLDatabaseConfig::fromArray($config);
    }
}
