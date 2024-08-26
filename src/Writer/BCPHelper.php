<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class BCPHelper
{
    public const STRING_TYPES = [
        'char', 'varchar', 'text',
        'nchar', 'nvarchar', 'ntext',
    ];

    public const TEXT_TYPES = [
        'text', 'ntext', 'image',
    ];

    public const BINARY_TYPES = [
        'binary', 'varbinary', 'image',
    ];

    public function __construct(private readonly MSSQLConnection $connection)
    {
    }

    /**
     * @param ItemConfig[] $columns
     * @return ItemConfig[]
     */
    public function convertColumns(array $columns): array
    {
        $newColumns = [];
        foreach ($columns as $column) {
            if ($column->getType() !== 'ignore') {
                $newColumns[] = $this->transformColumn($column);
            }
        }
        return $newColumns;
    }

    private function transformColumn(ItemConfig $column): ItemConfig
    {
        $size = '255';
        if ($this->isStringType($column->getType()) && $column->hasSize()) {
            $size = $column->getSize();
            if ($size > '4000') {
                $size = 'MAX';
            }
        }
        if ($this->isTextType($column->getType()) || $this->isBinaryType($column->getType())) {
            $size = 'MAX';
        }
        $this->connection->quoteIdentifier($column->getDbName());

        return ItemConfig::fromArray([
            'name' => $column->getName(),
            'dbName' => $column->getDbName(),
            'type' => 'NVARCHAR',
            'size' => $size,
            'nullable' => true,
        ]);
    }

    private function isStringType(string $type): bool
    {
        return in_array(
            strtolower($type),
            [
                'char', 'varchar',
                'nchar', 'nvarchar',
                'binary', 'varbinary',
            ],
        );
    }

    private function isTextType(string $type): bool
    {
        return in_array(strtolower($type), self::TEXT_TYPES);
    }

    private function isBinaryType(string $type): bool
    {
        return in_array(strtolower($type), self::BINARY_TYPES);
    }
}
