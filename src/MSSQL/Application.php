<?php

declare(strict_types=1);

namespace Keboola\DbWriter\MSSQL;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\MSSQL\Configuration\MssqlActionConfigRowDefinition;
use Keboola\DbWriter\MSSQL\Configuration\MssqlConfigDefinition;
use Keboola\DbWriter\MSSQL\Configuration\MssqlConfigRowDefinition;
use Keboola\DbWriter\Writer\MSSQL;

class Application extends \Keboola\DbWriter\Application
{
    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ?: 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new MssqlConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new MssqlConfigRowDefinition();
            } else {
                $configDefinition = new MssqlActionConfigRowDefinition();
            }
        }
        parent::__construct($config, $logger, $configDefinition);
    }

    public function writeFull(CsvFile $csv, array $tableConfig): void
    {
        /** @var MSSQL $writer */
        $writer = $this['writer'];

        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->write($csv, $tableConfig);
    }

    public function writeIncremental(CsvFile $csv, array $tableConfig): void
    {
        /** @var MSSQL $writer */
        $writer = $this['writer'];

        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);

        $writer->drop($stageTable['dbName']);
        $writer->create($stageTable);
        $writer->write($csv, $stageTable);

        // create destination table if not exists
        $dstTableExists = $writer->tableExists($tableConfig['dbName']);
        if (!$dstTableExists) {
            $writer->create($tableConfig);
        }
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }
}
