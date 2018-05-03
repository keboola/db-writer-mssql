<?php

namespace Keboola\DbWriter\Test;

use Keboola\DbWriter\Logger;
use Keboola\DbWriter\WriterFactory;
use PHPUnit\Framework\TestCase;

class MSSQLBaseTest extends TestCase
{
    protected $dataDir = __DIR__ . "/../../tests/data";

    /**
     * @param $driver
     * @return mixed
     * @throws \Exception
     */
    protected function getConfig($driver)
    {
        $config = json_decode(file_get_contents($this->dataDir . '/' . $driver . '/config.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

        return $config;
    }

    /**
     * @param $driver
     * @param $suffix
     * @param bool $required
     * @return array|false|string
     * @throws \Exception
     */
    protected function getEnv($driver, $suffix, $required = false)
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (false === getenv($env)) {
                throw new \Exception($env . " environment variable must be set.");
            }
        }
        return getenv($env);
    }

    /**
     * @param $parameters
     * @return \Keboola\DbWriter\WriterInterface
     * @throws \Keboola\DbWriter\Exception\UserException
     */
    protected function getWriter($parameters)
    {
        $writerFactory = new WriterFactory($parameters);

        return $writerFactory->create(new Logger("wr-db-mssql-test"));
    }
}
