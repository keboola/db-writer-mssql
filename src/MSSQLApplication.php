<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Component\Config\BaseConfig;
use Keboola\DbWriter\Configuration\NodeDefinition\MSSQLDbNode;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Configuration\ValueObject\MSSQLExportConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriterConfig\Configuration\ConfigDefinition;
use Keboola\DbWriterConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MSSQLApplication extends Application
{
    protected string $writerName = 'MSSQL';

    /**
     * @param $dbParams array{
     *     host: string,
     *     database: string,
     *     user: string,
     *     "#password": string,
     *     schema?: string,
     *     tdsVersion?: string,
     *     instance?: string,
     *     collation?: string,
     *     ssh?: array,
     * }
     */
    protected function createDatabaseConfig(array $dbParams): MSSQLDatabaseConfig
    {
        return MSSQLDatabaseConfig::fromArray($dbParams);
    }

    protected function loadConfig(): void
    {
        $configClass = $this->getConfigClass();
        $configDefinitionClass = $this->getConfigDefinitionClass();

        if (in_array($configDefinitionClass, [ConfigRowDefinition::class, ConfigDefinition::class])) {
            $definition = new $configDefinitionClass(
                dbNode: (new MSSQLDbNode())->ignoreExtraKeys(),
            );
        } else {
            $definition = new $configDefinitionClass(dbNode: new MSSQLDbNode());
        }

        try {
            /** @var BaseConfig $config */
            $config = new $configClass(
                $this->getRawConfig(),
                $definition,
            );
            $this->config = $config;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function createExportConfig(array $table): MSSQLExportConfig
    {
        /** @var MSSQLExportConfig $exportConfig */
        $exportConfig = MSSQLExportConfig::fromArray(
            $table,
            $this->getConfig()->getInputTables(),
            $this->createDatabaseConfig($table['db']),
        );

        return $exportConfig;
    }
}
