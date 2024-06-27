<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SshConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SslConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

readonly class MSSQLDatabaseConfig extends DatabaseConfig
{
    public function __construct(
        private ?string $host,
        private ?string $port,
        private string $database,
        private string $user,
        private ?string $password,
        private ?string $schema,
        private ?string $tdsVersion,
        private ?string $instance,
        private ?string $collation,
        private ?SshConfig $sshConfig,
        private ?SslConfig $sslConfig,
    ) {
        parent::__construct(
            $this->host,
            $this->port,
            $this->database,
            $this->user,
            $this->password,
            $this->schema,
            $this->sshConfig,
            $this->sslConfig,
        );
    }

    /**
     * @param $config array{
     *     host?: string,
     *     port?: string,
     *     database: string,
     *     user: string,
     *     "#password"?: string,
     *     schema?: string,
     *     tdsVersion?: string,
     *     instance?: string,
     *     collation?: string,
     *     ssh?: array
     * }
     */
    public static function fromArray(array $config): self
    {
        $sshEnabled = $config['ssh']['enabled'] ?? false;

        return new self(
            $config['host'] ?? null,
            $config['port'] ?? null,
            $config['database'],
            $config['user'],
            $config['#password'] ?? null,
            $config['schema'] ?? null,
            $config['tdsVersion'] ?? null,
            $config['instance'] ?? null,
            $config['collation'] ?? null,
            $sshEnabled ? SshConfig::fromArray($config['ssh']) : null,
            null,
        );
    }

    public function hasTdsVersion(): bool
    {
        return $this->tdsVersion !== null;
    }

    public function hasInstance(): bool
    {
        return $this->instance !== null;
    }

    public function hasCollation(): bool
    {
        return $this->collation !== null;
    }

    public function getTdsVersion(): string
    {
        if ($this->tdsVersion === null) {
            throw new PropertyNotSetException('Property "tdsVersion" is not set.');
        }
        return $this->tdsVersion;
    }

    public function getInstance(): string
    {
        if ($this->instance === null) {
            throw new PropertyNotSetException('Property "instance" is not set.');
        }
        return $this->instance;
    }

    public function getCollation(): string
    {
        if ($this->collation === null) {
            throw new PropertyNotSetException('Property "collation" is not set.');
        }
        return $this->collation;
    }
}
