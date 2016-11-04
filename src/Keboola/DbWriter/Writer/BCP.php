<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 04/11/16
 * Time: 16:34
 */

namespace Keboola\DbWriter\Writer;
use Symfony\Component\Process\Process;

/**
 * Class BCP
 * @package Keboola\DbWriter\Writer
 *
 * Wrapper for Bulk Copy `bcp` command line utility
 */
class BCP
{
    private $conn;

    private $dbParams;

    public function __construct(\PDO $conn, $dbParams)
    {
        $this->conn = $conn;
        $this->dbParams = $dbParams;
    }

    public function import($filename, $table)
    {
        $formatFile = $this->createFormatFile($table);
        $process = new Process($this->createBcpCommand($filename, $table, $formatFile));
        $process->setTimeout(3600*2);
        $process->run();

        return $process;
    }

    private function createBcpCommand($filename, $table, $formatFile)
    {
        return sprintf(
            'bcp %s in %s -t , -f %s -S "%s" -U %s -P %s -D %s -k',
            $table['schema'] . '.' . $table['dbName'],
            $filename,
            $formatFile,
            $this->dbParams['host'] . "," . $this->dbParams['port'],
            $this->dbParams['user'],
            $this->dbParams['password'],
            $this->dbParams['database']
        );
    }

    private function createFormatFile($table)
    {
        // get collation of the table
        $stmt = $this->conn->query("
            SELECT c.name, c.collation_name
            FROM SYS.COLUMNS c
            JOIN SYS.TABLES t ON t.object_id = c.object_id
            WHERE t.name = '{$table['dbName']}'
        ");
        $res = $stmt->fetchAll();
        $collation = $res['collation'];

        $driverVersion = "13.0";
        $columnsCount = count($table['items']) + 1;
        $prefixLength = 0;
        $sourceType = "SQLCHAR";
        $delimiter = '"\""';

        $formatData = $driverVersion . PHP_EOL;
        $formatData .= $columnsCount . PHP_EOL;
        // dummy column for the quote hack
        $formatData .= "1       {$sourceType}     {$prefixLength}       0       {$delimiter}       0       dummy       {$collation}" . PHP_EOL;

        $cnt = 1;
        foreach ($table['items'] as $column) {
            $cnt++;
            $dstCnt = $cnt - 1;

            $length = '255';
            if (strstr(strtolower($column['type']), 'char') !== false && !empty($column['size'])) {
                $length = $column['size'] * 2;
            }

            $delimiter = '"\",\""';

            if ($cnt >= $columnsCount) {
                $delimiter = '"\"\n"';
            }

            $formatData .= "{$cnt}      {$sourceType}     {$prefixLength}       {$length}       {$delimiter}       {$dstCnt}       {$column['dbName']}       {$collation}" . PHP_EOL;
        }

        $filename = ROOT_PATH . uniqid("format_file_{$table['dbName']}_");
        file_put_contents($filename, $formatData);

        return $filename;
    }
}
