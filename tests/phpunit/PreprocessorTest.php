<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Writer\Preprocessor;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class PreprocessorTest extends TestCase
{
    protected string $dataDir = __DIR__ . '/../../data';

    public function testProcess(): void
    {
        $tmp = new Temp();
        $sourceFile = $tmp->createFile('special.csv');
        file_put_contents(
            $sourceFile->getPathname(),
            <<<CSV
"col1","col2"
"line with enclosure","second column"
"column with enclosure "", and comma inside text","second column enclosure in text """
"columns with
new line","columns with 	tab"
"column with backslash \ inside","column with backslash and enclosure \"""
"column with \n \t \\","second col"
"single quote'","two single''quotes"
"first","something with

double new line"
CSV,
        );

        $processedFile = $tmp->createFile('special.processed');
        file_put_contents(
            $processedFile->getPathname(),
            <<<CSV
"col1"<~|~>"col2"
"line with enclosure"<~|~>"second column"
"column with enclosure ", and comma inside text"<~|~>"second column enclosure in text ""
"columns with
new line"<~|~>"columns with 	tab"
"column with backslash \ inside"<~|~>"column with backslash and enclosure \""
"column with \n \t \\"<~|~>"second col"
"single quote'"<~|~>"two single''quotes"
"first"<~|~>"something with

double new line"

CSV,
        );

        $inputCsv = new CsvReader($sourceFile->getPathname());
        $preprocessor = new Preprocessor(
            $inputCsv,
            [
                new ItemConfig('col1', 'col1', 'string', null, null, null),
                new ItemConfig('col2', 'col2', 'string', null, null, null),
            ],
        );
        $outFilename = $preprocessor->process('special');

        $this->assertFileEquals($processedFile->getPathname(), $outFilename);
    }
}
