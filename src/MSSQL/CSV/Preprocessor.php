<?php
/**
 * Author: miro@keboola.com
 * Date: 06/04/2017
 */
namespace Keboola\DbWriter\MSSQL\CSV;

use Keboola\Csv\CsvFile;

class Preprocessor
{
    protected $input;

    protected $output;

    protected $delimiter = '<~|~>';

    protected $enclosure = '"';

    protected $tmpDir = '/tmp';

    public function __construct(CsvFile $input)
    {
        $this->input = $input;
        $this->input->rewind();
    }

    public function setDelimiter($delimiter)
    {
        $this->delimiter = $delimiter;
    }

    /**
     * Replaces delimiter with set delimiter
     *
     * @return string output filename
     * @throws \Exception
     */
    public function process()
    {
        $outFilename = tempnam($this->tmpDir, $this->input->getFilename());
        $fh = fopen($outFilename, 'w');

        while ($this->input->current() !== false) {
            $row = $this->input->current();
            fwrite($fh, $this->rowToStr($row));
            $this->input->next();
        }

        return $outFilename;
    }

    protected function rowToStr(array $row)
    {
        $return = array();
        foreach ($row as $column) {
            if (!is_scalar($column) && !is_null($column)) {
                $type = gettype($column);
                throw new \Exception("Cannot write {$type} into a column");
            }

            $return[] = $this->enclosure . $column . $this->enclosure;
        }

        return implode($this->delimiter, $return) . "\n";
    }
}
