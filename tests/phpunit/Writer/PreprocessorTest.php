<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests\Writer;

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Writer\Preprocessor;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\TestCase;

class PreprocessorTest extends TestCase
{
    protected string $dataDir = __DIR__ . '/../../data';

    public function testProcess(): void
    {
        $inputCsv = new CsvReader($this->dataDir . '/special.csv');
        $preprocessor = new Preprocessor(
            $inputCsv,
            [
                new ItemConfig('col1', 'col1', 'string', null, null, null),
                new ItemConfig('col2', 'col2', 'string', null, null, null),
            ],
        );
        $outFilename = $preprocessor->process('special');

        $expected = $this->dataDir . '/special.processed';
        $this->assertFileEquals($expected, $outFilename);
    }
}
