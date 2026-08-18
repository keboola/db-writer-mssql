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
            $databaseConfig->getConnectionUsername(),
            $databaseConfig->getConnectionPassword(),
            [],
        );
    }

    public static function buildDsn(MSSQLDatabaseConfig $databaseConfig): string
    {
        $host = $databaseConfig->getHost();
        if ($databaseConfig->hasPort() && $databaseConfig->getPort() !== '1433') {
            $host .= ',' . $databaseConfig->getPort();
        }
        if ($databaseConfig->hasInstance()) {
            $host .= '\\' . $databaseConfig->getInstance();
        }

        $options = [
            'Server' => $host,
            'Database' => $databaseConfig->getDatabase(),
        ];

        if ($databaseConfig->hasServicePrincipal()) {
            // Microsoft Entra ID Service Principal: the driver reads UID/PWD as client id/secret.
            // The tenant is inferred from the target server, so tenantId is not needed here.
            $options['Authentication'] = 'ActiveDirectoryServicePrincipal';
            $options['Encrypt'] = 'true';
        } else {
            // ODBC Driver 18 defaults to mandatory TLS encryption with full server certificate
            // validation, whereas Driver 17 did not encrypt by default. The writer exposes no SSL
            // configuration, so trust the server certificate to keep existing SQL login configs
            // (typically on-prem servers with self-signed certificates) working. This mirrors the
            // bcp import path in BCP::getTrustServerCertificateFlag() (the `-u` option).
            $options['TrustServerCertificate'] = 'true';
        }

        return 'sqlsrv:' . implode(';', array_map(
            fn(string $key, string $value): string => $key . '=' . $value,
            array_keys($options),
            $options,
        ));
    }
}
