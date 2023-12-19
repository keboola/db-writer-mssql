<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Tests\MSSQL\CSV;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\MSSQL\CSV\Preprocessor;
use PHPUnit\Framework\TestCase;

class PreprocessorTest extends TestCase
{
    /** @var string */
    protected $dataDir = __DIR__ . '/../../../data';

    public function testProcess(): void
    {
        $inputCsv = new CsvFile($this->dataDir . '/special.csv');
        $preprocessor = new Preprocessor(
            $inputCsv,
            [
                ['name' => 'col1'],
                ['name' => 'col2'],
            ]
        );
        $outFilename = $preprocessor->process();

        $expected = $this->dataDir . '/special.processed';
        $this->assertFileEquals($expected, $outFilename);
    }

    public function testProcessSkipColumn(): void
    {
        $inputCsv = new CsvFile($this->dataDir . '/special.csv');
        $preprocessor = new Preprocessor(
            $inputCsv,
            [
                ['name' => 'col1'],
            ]
        );
        $outFilename = $preprocessor->process();

        $expected = $this->dataDir . '/special.skip-column-processed';
        $this->assertFileEquals($expected, $outFilename);
    }
}
