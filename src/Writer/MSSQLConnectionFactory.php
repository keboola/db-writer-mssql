<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterAdapter\PDO\PdoConnection;
use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Psr\Log\LoggerInterface;

class MSSQLConnectionFactory
{
    public function __construct(readonly private LoggerInterface $logger)
    {
    }

    public function create(MSSQLDatabaseConfig $databaseConfig): MSSQLConnection
    {
        $dsn = self::buildDsn($databaseConfig);

        return new MSSQLConnection(
            $this->logger,
            $dsn,
            $databaseConfig->getUser(),
            $databaseConfig->getPassword(),
            [],
        );
    }

    private function buildDsn(MSSQLDatabaseConfig $databaseConfig): string
    {
        $host = $databaseConfig->getHost();
        if ($databaseConfig->hasPort() && $databaseConfig->getPort() !== '1433') {
            $host .= ',' . $databaseConfig->getPort();
        }
        if ($databaseConfig->hasInstance()) {
            $host .= '\\' . $databaseConfig->getInstance();
        }

        return sprintf(
            'sqlsrv:Server=%s;Database=%s',
            $host,
            $databaseConfig->getDatabase(),
        );
    }
}
