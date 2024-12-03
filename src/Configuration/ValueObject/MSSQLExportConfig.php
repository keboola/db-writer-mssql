<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriterConfig\Exception\UserException;

readonly class MSSQLExportConfig extends ExportConfig
{

    /**
     * @param $config array{
     *     data_dir: string,
     *     writer_class: string,
     *     db: array,
     *     tableId: string,
     *     dbName: string,
     *     incremental?: bool,
     *     export?: bool,
     *     primaryKey?: string[],
     *     items: array
     * }
     * @param $inputMapping array{
     *     source: string,
     *     destination: string,
     *     columns: array<string>
     * }
     */
    public static function fromArray(array $config, array $inputMapping, ?DatabaseConfig $databaseConfig = null): self
    {
        $filteredInputMapping = array_filter($inputMapping, fn($v) => $v['source'] === $config['tableId']);
        if (count($filteredInputMapping) === 0) {
            throw new UserException(
                sprintf('Table "%s" in storage input mapping cannot be found.', $config['tableId']),
            );
        }
        $tableFilePath = sprintf(
            '%s/in/tables/%s',
            $config['data_dir'],
            current($filteredInputMapping)['destination'],
        );

        return new self(
            $config['data_dir'],
            $config['writer_class'],
            $databaseConfig ?? DatabaseConfig::fromArray($config['db']),
            $config['tableId'],
            $config['dbName'],
            $config['incremental'] ?? false,
            $config['export'] ?? true,
            !empty($config['primaryKey']) ? $config['primaryKey'] : null,
            array_map(fn($v) => ItemConfig::fromArray($v), $config['items']),
            $tableFilePath,
        );
    }

    public function getDbName(): string
    {
        if (!$this->getDatabaseConfig()->hasSchema()) {
            return parent::getDbName();
        }
        if (str_contains(parent::getDbName(), '.')) {
            return parent::getDbName();
        }
        return $this->getDatabaseConfig()->getSchema() . '.' . parent::getDbName();
    }
}
