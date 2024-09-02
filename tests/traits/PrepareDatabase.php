<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\TraitTests;

use Keboola\DbWriter\Writer\MSSQLConnection;

trait PrepareDatabase
{
    private const BASIC_USER_LOGIN = 'basicUser';
    private const BASIC_USER_PASSWORD = 'Abcdefg1234';
    private const NO_PERM_USER_LOGIN = 'noPerm';
    private const NO_PERM_USER_PASSWORD = 'pwd12334$%^&';

    public function prepareDatabase(MSSQLConnection $connection, string $database): void
    {
        // Drop database
        $connection->exec('USE master');
        $connection->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s')
            ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        ", $database, $database));
        $connection->exec(sprintf("
            IF EXISTS(select * from sys.databases where name='%s') 
            DROP DATABASE %s
        ", $database, $database));

        // Create database
        $connection->exec(sprintf('CREATE DATABASE %s COLLATE CZECH_CI_AS', $database));
        $connection->exec(sprintf('USE %s', $database));

        // Drop users
        $connection->exec(sprintf('DROP USER IF EXISTS %s', self::BASIC_USER_LOGIN));
        $connection->exec(sprintf("
            IF EXISTS (SELECT * FROM sys.syslogins WHERE name = N'%s')
            DROP LOGIN %s
        ", self::BASIC_USER_LOGIN, self::BASIC_USER_LOGIN));
        $connection->exec(sprintf('DROP USER IF EXISTS %s', self::NO_PERM_USER_LOGIN));
        $connection->exec(sprintf("
            IF  EXISTS (SELECT * FROM sys.syslogins WHERE name = N'%s')
            DROP LOGIN %s
        ", self::NO_PERM_USER_LOGIN, self::NO_PERM_USER_LOGIN));

        // Create login and users
        $connection->exec(sprintf(
            "CREATE LOGIN %s WITH PASSWORD = '%s'",
            self::BASIC_USER_LOGIN,
            self::BASIC_USER_PASSWORD,
        ));
        $connection->exec(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            self::BASIC_USER_LOGIN,
            self::BASIC_USER_LOGIN,
        ));
        $connection->exec(sprintf(
            'GRANT CONTROL ON DATABASE::%s TO %s',
            $database,
            self::BASIC_USER_LOGIN,
        ));
        $connection->exec(sprintf(
            'GRANT CONTROL ON SCHEMA::%s TO %s',
            'dbo',
            self::BASIC_USER_LOGIN,
        ));
        $connection->exec(sprintf(
            'REVOKE EXECUTE TO %s',
            self::BASIC_USER_LOGIN,
        ));
        $connection->exec(sprintf(
            "CREATE LOGIN %s WITH PASSWORD = '%s'",
            self::NO_PERM_USER_LOGIN,
            self::NO_PERM_USER_PASSWORD,
        ));

        $connection->exec(sprintf(
            'CREATE USER %s FOR LOGIN %s',
            self::NO_PERM_USER_LOGIN,
            self::NO_PERM_USER_LOGIN,
        ));
        $connection->exec(sprintf(
            'REVOKE EXECUTE TO %s',
            self::NO_PERM_USER_LOGIN,
        ));

        $connection->exec('USE test');

        $this->cleanup($connection);
    }


    private function cleanup(MSSQLConnection $connection): void
    {
        $tables = $connection->fetchAll('SELECT * FROM INFORMATION_SCHEMA.TABLES;');
        foreach ($tables as $table) {
            if (!is_string($table['TABLE_NAME'])) {
                continue;
            }
            $connection->exec(
                sprintf(
                    "IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s",
                    $connection->quoteIdentifier($table['TABLE_NAME']),
                    $connection->quoteIdentifier($table['TABLE_NAME']),
                ),
            );
        }
    }
}
