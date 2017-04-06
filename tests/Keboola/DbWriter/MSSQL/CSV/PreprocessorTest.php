<?php
/**
 * Author: miro@keboola.com
 * Date: 06/04/2017
 */
namespace Keboola\DbWriter\MSSQL\CSV;

use Keboola\Csv\CsvFile;

class PreprocessorTest extends \PHPUnit_Framework_TestCase
{
    protected $dataDir = ROOT_PATH . "/tests/data";

    public function testProcess()
    {
        $inputCsv = new CsvFile($this->dataDir . '/special.csv');
        $preprocessor = new Preprocessor($inputCsv);
        $outFilename = $preprocessor->process();

        $expected = $this->dataDir . '/special.processed';
        $this->assertFileEquals($expected, $outFilename);
    }
}