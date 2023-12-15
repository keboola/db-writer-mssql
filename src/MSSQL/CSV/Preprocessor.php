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

    /** @var array */
    private $items;

    public function __construct(CsvFile $input, array $items)
    {
        $this->input = $input;
        $this->items = array_map(function (array $item) {
            return $item['name'];
        }, $items);
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
        $header = $this->input->getHeader();

        $headerNotLoaded = array_diff($header, $this->items);

        while ($this->input->current() !== false) {
            $row = $this->input->current();
            fwrite($fh, $this->rowToStr($row, $headerNotLoaded));
            $this->input->next();
        }

        return $outFilename;
    }

    protected function rowToStr(array $row, array $headerNotLoaded): string
    {
        $return = array();
        foreach ($row as $key => $column) {
            if (array_key_exists($key, $headerNotLoaded)) {
                continue;
            }
            if (!is_scalar($column) && !is_null($column)) {
                $type = gettype($column);
                throw new \Exception("Cannot write {$type} into a column");
            }

            $return[] = $this->enclosure . $column . $this->enclosure;
        }

        return implode($this->delimiter, $return) . "\n";
    }
}
