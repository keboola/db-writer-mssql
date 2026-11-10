<?php

declare(strict_types=1);

use Keboola\DbWriter\Mssql\FunctionalTests\DatadirTest;

return function (DatadirTest $test): void {
    $test->connection->exec('CREATE TABLE simple (
        id INT,
        name VARCHAR(255),
        glasses VARCHAR(255)
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

    $addDescription = 'EXEC sys.sp_addextendedproperty @name = N\'MS_Description\', @value = N\'%s\', '
        . '@level0type = N\'SCHEMA\', @level0name = N\'dbo\', @level1type = N\'TABLE\', @level1name = N\'simple\'%s';
    $test->connection->exec(sprintf($addDescription, 'Existing table description', ''));
    $test->connection->exec(sprintf(
        $addDescription,
        'Existing name description',
        ', @level2type = N\'COLUMN\', @level2name = N\'name\'',
    ));
    $test->connection->exec(sprintf(
        $addDescription,
        'Existing glasses description',
        ', @level2type = N\'COLUMN\', @level2name = N\'glasses\'',
    ));

    $test->dumpComments = true;
};
