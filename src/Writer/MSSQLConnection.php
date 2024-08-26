<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\PDO\PdoConnection;

class MSSQLConnection extends PdoConnection
{
    public function testConnection(): void
    {
        $this->exec('SELECT GETDATE() AS CurrentDateTime');
    }

    public function quoteIdentifier(string $str): string
    {
        $objNameArr = explode('.', $str);
        if (count($objNameArr) > 1) {
            return $objNameArr[0] . '.[' . $objNameArr[1] . ']';
        }

        return '[' . $objNameArr[0] . ']';
    }
}
