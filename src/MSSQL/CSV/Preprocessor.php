<?php

declare(strict_types=1);

namespace Keboola\DbWriter\MSSQL\CSV;

use Keboola\Csv\CsvFile;

class Preprocessor
{
    /** @var CsvFile */
    protected $input;

    /** @var string */
    protected $delimiter = '<~|~>';

    /** @var string */
    protected $enclosure = '"';

    /** @var string */
    protected $tmpDir = '/tmp';

    public function __construct(CsvFile $input)
    {
        $this->input = $input;
        $this->input->rewind();
    }

    public function setDelimiter(string $delimiter): void
    {
        $this->delimiter = $delimiter;
    }

    public function process(): string
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

    protected function rowToStr(array $row): string
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
