<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Symfony\Component\Process\Process;

class BCP
{
    /** @var \PDO */
    private $conn;

    /** @var array */
    private $dbParams;

    /** @var Logger */
    private $logger;

    /** @var string */
    private $delimiter = '<~|~>';

    /** @var string */
    private $errorFile = '/tmp/wr-db-mssql-errors';

    public function __construct(\PDO $conn, array $dbParams, Logger $logger)
    {
        $this->conn = $conn;
        $this->dbParams = $dbParams;
        $this->logger = $logger;
        @unlink($this->errorFile);
    }

    public function setDelimiter(string $delimiter): void
    {
        $this->delimiter = $delimiter;
    }

    public function import(string $filename, array $table): void
    {
        $formatFile = $this->createFormatFile($table);

        $process = new Process($this->createBcpCommand($filename, $table, $formatFile));
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
                $errors
            ));
        }

        @unlink($formatFile);
    }

    private function createBcpCommand(string $filename, array $table, string $formatFile): array
    {
        $serverName = $this->dbParams['host'];
        $serverName .= !empty($this->dbParams['instance']) ? '\\' . $this->dbParams['instance'] : '';
        $serverName .= ',' . $this->dbParams['port'];

        $cmd = [
            'bcp',
            $table['dbName'],
            'in',
            $filename,
            '-f',
            $formatFile,
            '-S',
            $serverName,
            '-U',
            $this->dbParams['user'],
            '-P',
            $this->dbParams['#password'],
            '-d',
            $this->dbParams['database'],
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
            json_encode($log)
        ));

        return $cmd;
    }

    private function createFormatFile(array $table): string
    {
        $collation = $this->getCollation();
        $serverVersion = $this->getVersion();
        $columnsCount = count($table['items']) + 1;
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
        foreach ($table['items'] as $column) {
            $cnt++;
            $dstCnt = $cnt - 1;

            $delimiter = '"\"' . $this->delimiter . '\""';

            if ($cnt >= $columnsCount) {
                $delimiter = '"\"\n"';
            }

            // phpcs:ignore
            $formatData .= "{$cnt}      {$sourceType}     {$prefixLength}       {$length}       {$delimiter}       {$dstCnt}       {$column['dbName']}       {$collation}" . PHP_EOL;
        }

        $this->logger->info('Format file: ' . PHP_EOL . $formatData);

        $filename = '/tmp' . uniqid("format_file_{$table['dbName']}_");
        file_put_contents($filename, $formatData);

        return $filename;
    }

    private function getVersion(): string
    {
        $stmt = $this->conn->query("SELECT CONVERT (varchar, SERVERPROPERTY('ProductMajorVersion'))");
        $res = $stmt->fetchAll();
        $version = $res[0][0];

        if (empty($version)) {
            $version = 12;
        }

        return $version . '.0';
    }

    private function getCollation(): string
    {
        if (!empty($this->dbParams['collation'])) {
            return $this->dbParams['collation'];
        }
        $stmt = $this->conn->query("SELECT CONVERT (varchar, SERVERPROPERTY('collation'))");
        $res = $stmt->fetchAll();
        $collation = $res[0][0];

        if (empty($collation)) {
            return 'SQL_Latin1_General_CP1_CI_AS';
        }

        return $collation;
    }

    private function escape(string $obj): string
    {
        $objNameArr = explode('.', $obj);
        if (count($objNameArr) > 1) {
            return $objNameArr[0] . '.[' . $objNameArr[1] . ']';
        }

        return '[' . $objNameArr[0] . ']';
    }

    private function escapeSpecialChars(string $str): string
    {
        return preg_replace('/([$])/', '\\\\${1}', $str);
    }
}
