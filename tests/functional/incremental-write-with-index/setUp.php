<?php

declare(strict_types=1);

use Keboola\DbWriter\Mssql\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->connection->exec('CREATE TABLE simple (
        id INT,
        name VARCHAR(255),
        glasses VARCHAR(255),
        CONSTRAINT [someIndexNameId] PRIMARY KEY CLUSTERED (id)
    )');

    // insert 100 row to table and different all values
    for ($i = 1; $i <= 20; $i++) {
        $test->connection->exec(sprintf(
            'INSERT INTO simple VALUES (\'%d\', \'name%d\', \'glasses%d\')',
            $i,
            $i,
            $i,
        ));
    }
};
