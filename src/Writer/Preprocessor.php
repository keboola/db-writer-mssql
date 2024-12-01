<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Exception;
use Keboola\Csv\CsvReader;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class Preprocessor
{
    protected CsvReader $input;

    protected string $delimiter = '<~|~>';

    protected string $enclosure = '"';

    protected string $tmpDir = '/tmp';

    /** @var string[] $items */
    private array $items;

    /**
     * @param ItemConfig[] $items
     */
    public function __construct(CsvReader $input, array $items)
    {
        $this->input = $input;
        $this->items = array_map(function (ItemConfig $item) {
            return $item->getName();
        }, $items);
        $this->input->rewind();
    }

    public function process(string $tableName): string
    {
        $outFilename = (string) tempnam($this->tmpDir, $tableName);
        $fh = fopen($outFilename, 'w');
        if ($fh === false) {
            throw new ApplicationException('Cannot open file for writing');
        }
        $header = $this->input->getHeader();

        $excludeColumns = array_diff($header, $this->items);

        while ($this->input->current() !== false) {
            $row = (array) $this->input->current();
            fwrite($fh, $this->rowToStr($row, $excludeColumns));
            $this->input->next();
        }

        return $outFilename;
    }

    /**
     * @param mixed[] $row
     * @param mixed[] $excludeColumns
     */
    protected function rowToStr(array $row, array $excludeColumns): string
    {
        $return = [];
        foreach ($row as $key => $column) {
            if (array_key_exists($key, $excludeColumns)) {
                continue;
            }
            if (!is_scalar($column) && !is_null($column)) {
                $type = gettype($column);
                throw new Exception("Cannot write {$type} into a column");
            }

            $return[] = $this->enclosure . $column . $this->enclosure;
        }

        return implode($this->delimiter, $return) . "\n";
    }
}
