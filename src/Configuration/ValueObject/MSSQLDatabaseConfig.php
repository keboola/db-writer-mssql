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
        private ?string $tenantId = null,
        private ?string $clientId = null,
        private ?string $clientSecret = null,
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
     *     user?: string,
     *     "#password"?: string,
     *     schema?: string,
     *     tdsVersion?: string,
     *     instance?: string,
     *     collation?: string,
     *     tenantId?: string,
     *     clientId?: string,
     *     "#clientSecret"?: string,
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
            $config['user'] ?? '',
            $config['#password'] ?? null,
            $config['schema'] ?? null,
            $config['tdsVersion'] ?? null,
            $config['instance'] ?? null,
            $config['collation'] ?? null,
            $sshEnabled ? SshConfig::fromArray($config['ssh']) : null,
            null,
            $config['tenantId'] ?? null,
            $config['clientId'] ?? null,
            $config['#clientSecret'] ?? null,
        );
    }

    /**
     * SshTunnel rebuilds the config via toArray()/fromArray(), so the Service Principal
     * credentials have to survive the round trip - otherwise an SSH-tunnelled config would
     * silently lose them and fall back to an empty SQL login.
     *
     * @return array<string, mixed>
     */
    public function toArray(): array
    {
        return array_merge(parent::toArray(), [
            'tenantId' => $this->tenantId,
            'clientId' => $this->clientId,
            '#clientSecret' => $this->clientSecret,
        ]);
    }

    /**
     * Microsoft Entra ID Service Principal auth is used when all three credentials are present.
     */
    public function hasServicePrincipal(): bool
    {
        return $this->tenantId !== null && $this->clientId !== null && $this->clientSecret !== null;
    }

    public function getTenantId(): string
    {
        if ($this->tenantId === null) {
            throw new PropertyNotSetException('Property "tenantId" is not set.');
        }
        return $this->tenantId;
    }

    public function getClientId(): string
    {
        if ($this->clientId === null) {
            throw new PropertyNotSetException('Property "clientId" is not set.');
        }
        return $this->clientId;
    }

    public function getClientSecret(): string
    {
        if ($this->clientSecret === null) {
            throw new PropertyNotSetException('Property "#clientSecret" is not set.');
        }
        return $this->clientSecret;
    }

    /**
     * Username handed to the PDO/ODBC driver. For Service Principal auth this is the client ID.
     */
    public function getConnectionUsername(): string
    {
        return $this->hasServicePrincipal() ? $this->getClientId() : $this->getUser();
    }

    /**
     * Password handed to the PDO/ODBC driver. For Service Principal auth this is the client secret.
     */
    public function getConnectionPassword(): string
    {
        return $this->hasServicePrincipal() ? $this->getClientSecret() : $this->getPassword();
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
