<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

class SQLTransformer
{
    public static function convertColumns(array $columnDefinitions): array
    {
        $sqlColumns = [];
        foreach ($columnDefinitions as $column) {
            if ($column['type'] !== 'ignore') {
                $sqlColumns[] = self::transformColumn($column);
            }
        }
        return $sqlColumns;
    }

    private static function transformColumn(array $column): string
    {
        $size = '255';
        if (self::isStringType($column['type']) && !empty($column['size'])) {
            $size = $column['size'];
            if ($size > '4000') {
                $size = 'MAX';
            }
        }
        if (self::isTextType($column['type']) || self::isBinaryType($column['type'])) {
            $size = 'MAX';
        }

        return sprintf('%s NVARCHAR (%s) NULL', self::escape($column['dbName']), $size);
    }

    private static function isStringType(string $type): bool
    {
        return in_array(
            strtolower($type),
            [
                'char', 'varchar',
                'nchar', 'nvarchar',
                'binary', 'varbinary',
            ]
        );
    }

    private static function isTextType(string $type): bool
    {
        return in_array(strtolower($type), MSSQL::$textTypes);
    }

    private static function isBinaryType(string $type): bool
    {
        return in_array(strtolower($type), MSSQL::$binaryTypes);
    }

    public static function escape(string $obj): string
    {
        $objNameArr = explode('.', $obj);
        if (count($objNameArr) > 1) {
            return $objNameArr[0] . '.[' . $objNameArr[1] . ']';
        }

        return '[' . $objNameArr[0] . ']';
    }
}
