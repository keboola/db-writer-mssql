<?php

declare(strict_types=1);

use Keboola\DbWriter\Mssql\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->connection->exec('CREATE TABLE simple (
        id INT,
        name VARCHAR(255),
        glasses VARCHAR(255)
    )');

    $test->connection->exec('GRANT CONTROL ON DATABASE::test TO noPerm');
    $test->connection->exec('GRANT CONTROL ON SCHEMA::dbo TO noPerm');
    $test->connection->exec('DENY INSERT ON OBJECT::dbo.simple TO noPerm');
};
