<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

class BCP
{
    private string $delimiter = '<~|~>';

    private string $errorFile = '/tmp/wr-db-mssql-errors';

    public function __construct(
        readonly private MSSQLConnection $connection,
        readonly private MSSQLDatabaseConfig $databaseConfig,
        readonly private LoggerInterface $logger,
    ) {
    }

    /**
     * @param ItemConfig[] $items
     */
    public function import(string $filename, string $tableName, array $items): void
    {
        @unlink($this->errorFile);

        $formatFile = $this->createFormatFile($tableName, $items);

        $process = new Process($this->createBcpCommand($filename, $tableName, $formatFile));
        $process->setTimeout(null);
        $process->run();

        if (!$process->isSuccessful()) {
            $errors = '';
            if (file_exists($this->errorFile)) {
                $errors = file_get_contents($this->errorFile);
            }

            throw new UserException(sprintf(
                "Import process failed. Output: %s. \n\n Error Output: %s. \n\n Errors: %s",
                $process->getOutput(),
                $process->getErrorOutput(),
                $errors,
            ));
        }

        @unlink($formatFile);
    }

    /** @return string[] */
    private function createBcpCommand(string $filename, string $tableName, string $formatFile): array
    {
        $serverName = $this->databaseConfig->getHost();
        $serverName .= $this->databaseConfig->hasInstance() ? '\\' . $this->databaseConfig->getInstance() : '';
        $serverName .= ',' . $this->databaseConfig->getPort();

        $cmd = [
            'bcp',
            $this->connection->quoteIdentifier($tableName),
            'in',
            $filename,
            '-f',
            $formatFile,
            '-S',
            $serverName,
            '-U',
            $this->databaseConfig->getUser(),
            '-P',
            $this->databaseConfig->getPassword(),
            '-d',
            $this->databaseConfig->getDatabase(),
            '-k',
            '-F2',
            '-b50000',
            '-e',
            $this->errorFile,
            '-m1',
        ];

        $log = $cmd;
        $log[11] = '*****';
        $this->logger->info(sprintf(
            'Executing BCP command: %s',
            json_encode($log),
        ));

        return $cmd;
    }

    /**
     * @param ItemConfig[] $items
     */
    private function createFormatFile(string $tableName, array $items): string
    {
        $collation = $this->getCollation();
        $serverVersion = $this->getVersion();
        $columnsCount = count($items) + 1;
        $prefixLength = 0;
        $length = 0;
        $sourceType = 'SQLCHAR';

        $delimiter = '"\""';

        $formatData = $serverVersion . PHP_EOL;
        $formatData .= $columnsCount . PHP_EOL;

        // dummy column for the quote hack
        // phpcs:ignore
        $formatData .= "1       {$sourceType}     {$prefixLength}       0       {$delimiter}       0       dummy       {$collation}" . PHP_EOL;

        $cnt = 1;
        foreach ($items as $column) {
            $cnt++;
            $dstCnt = $cnt - 1;

            $delimiter = '"\"' . $this->delimiter . '\""';

            if ($cnt >= $columnsCount) {
                $delimiter = '"\"\n"';
            }

            // phpcs:ignore
            $formatData .= "{$cnt}      {$sourceType}     {$prefixLength}       {$length}       {$delimiter}       {$dstCnt}       {$column->getDbName()}       {$collation}" . PHP_EOL;
        }

        $this->logger->info('Format file: ' . PHP_EOL . $formatData);

        if (str_contains($tableName, '.')) {
            $tableName = explode('.', $tableName)[1];
        }
        $filename = '/tmp' . uniqid("format_file_{$tableName}_");
        file_put_contents($filename, $formatData);

        return $filename;
    }

    private function getVersion(): string
    {
        $res = $this->connection->fetchAll(
            "SELECT CONVERT (varchar, SERVERPROPERTY('ProductMajorVersion')) as version;",
        );
        $version = $res[0]['version'];

        if (empty($version)) {
            $version = 12;
        }

        // fixes "Unknown error occurred while attempting to read"
        if ($version > 14) {
            $version = 14;
        }

        return $version . '.0';
    }

    private function getCollation(): string
    {
        if ($this->databaseConfig->hasCollation()) {
            return $this->databaseConfig->getCollation();
        }
        /** @var array{"collation": string}[] $res */
        $res = $this->connection->fetchAll("SELECT CONVERT (varchar, SERVERPROPERTY('collation')) as collation");
        $collation = $res[0]['collation'];

        if (empty($collation)) {
            return 'SQL_Latin1_General_CP1_CI_AS';
        }

        return $collation;
    }
}
