<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Mssql\Tests;

use Keboola\DbWriter\Configuration\ValueObject\MSSQLDatabaseConfig;
use Keboola\DbWriter\Writer\BCP;
use Keboola\DbWriter\Writer\MSSQLConnection;
use Keboola\DbWriter\Writer\MSSQLConnectionFactory;
use Keboola\DbWriter\Writer\ServicePrincipalTokenProvider;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ServicePrincipalAuthTest extends TestCase
{
    public function testSqlLoginDsnTrustsServerCertificate(): void
    {
        // ODBC Driver 18 mandates TLS, so SQL logins must keep trusting self-signed certificates.
        self::assertSame(
            'sqlsrv:Server=mssql;Database=test;TrustServerCertificate=true',
            MSSQLConnectionFactory::buildDsn($this->createConfig()),
        );
    }

    public function testServicePrincipalDsn(): void
    {
        self::assertSame(
            'sqlsrv:Server=mssql;Database=test;Authentication=ActiveDirectoryServicePrincipal;Encrypt=true',
            MSSQLConnectionFactory::buildDsn($this->createServicePrincipalConfig()),
        );
    }

    public function testServicePrincipalCredentialsAreMappedToUidAndPwd(): void
    {
        $config = $this->createServicePrincipalConfig();

        self::assertTrue($config->hasServicePrincipal());
        self::assertSame('client-id', $config->getConnectionUsername());
        self::assertSame('client-secret', $config->getConnectionPassword());
    }

    public function testSqlLoginCredentialsAreUnchanged(): void
    {
        $config = $this->createConfig();

        self::assertFalse($config->hasServicePrincipal());
        self::assertSame('sa', $config->getConnectionUsername());
        self::assertSame('password', $config->getConnectionPassword());
    }

    public function testPartialServicePrincipalIsNotDetected(): void
    {
        $config = $this->createServicePrincipalConfig(clientSecret: null);

        self::assertFalse($config->hasServicePrincipal());
    }

    public function testBlankedOutServicePrincipalFallsBackToSqlLogin(): void
    {
        // A config switched back to a SQL login by emptying the fields rather than removing them
        // must not select Service Principal auth and then fail on the token request.
        $config = MSSQLDatabaseConfig::fromArray([
            'host' => 'mssql',
            'port' => '1433',
            'database' => 'test',
            'user' => 'sa',
            '#password' => 'password',
            'tenantId' => '',
            'clientId' => '',
            '#clientSecret' => '',
        ]);

        self::assertFalse($config->hasServicePrincipal());
        self::assertSame('sa', $config->getConnectionUsername());
        self::assertStringNotContainsString('Authentication=', MSSQLConnectionFactory::buildDsn($config));
    }

    public function testServicePrincipalSurvivesToArrayRoundTrip(): void
    {
        // SshTunnel rebuilds the config through toArray()/fromArray().
        $config = MSSQLDatabaseConfig::fromArray([
            'host' => 'server.database.windows.net',
            'port' => '1433',
            'database' => 'test',
            'tenantId' => 'tenant-id',
            'clientId' => 'client-id',
            '#clientSecret' => 'client-secret',
        ]);

        $roundTripped = MSSQLDatabaseConfig::fromArray($config->toArray());

        self::assertTrue($roundTripped->hasServicePrincipal());
        self::assertSame('client-id', $roundTripped->getConnectionUsername());
        self::assertSame('client-secret', $roundTripped->getConnectionPassword());
    }

    public function testBcpCommandUsesSqlLogin(): void
    {
        $cmd = $this->createBcp($this->createConfig())->createBcpCommand('/tmp/data', 'simple', '/tmp/format');

        self::assertContains('-U', $cmd);
        self::assertNotContains('-G', $cmd);
        // Mirrors TrustServerCertificate=true on the PDO path.
        self::assertContains('-u', $cmd);
        self::assertSame(['-U', 'sa', '-P', 'password', '-u'], array_slice($cmd, 8, 5));
    }

    public function testBcpCommandUsesAccessTokenFileForServicePrincipal(): void
    {
        $bcp = $this->createBcp($this->createServicePrincipalConfig(), 'the-access-token');
        $cmd = $bcp->createBcpCommand('/tmp/data', 'simple', '/tmp/format');

        // bcp has no Service Principal mode; it reads a Microsoft Entra ID token from a file.
        self::assertNotContains('-U', $cmd);
        self::assertSame('-G', $cmd[8]);
        self::assertSame('-P', $cmd[9]);

        $tokenFile = $cmd[10];
        self::assertFileExists($tokenFile);
        self::assertSame(
            'the-access-token',
            (string) mb_convert_encoding((string) file_get_contents($tokenFile), 'UTF-8', 'UTF-16LE'),
        );

        unlink($tokenFile);
    }

    public function testTokenFileIsUtf16LeWithoutBomAndNotWorldReadable(): void
    {
        $tokenFile = ServicePrincipalTokenProvider::createTokenFile("token\n");

        $contents = (string) file_get_contents($tokenFile);
        self::assertStringStartsNotWith("\xFF\xFE", $contents);
        self::assertSame("t\x00o\x00k\x00e\x00n\x00", $contents);
        self::assertSame('0600', substr(sprintf('%o', (int) fileperms($tokenFile)), -4));

        unlink($tokenFile);
    }

    public function testCredentialsAreMaskedInLog(): void
    {
        self::assertSame(
            ['bcp', '-U', 'sa', '-P', '*****', '-u'],
            BCP::maskCredentials(['bcp', '-U', 'sa', '-P', 'secret', '-u']),
        );
        self::assertSame(
            ['bcp', '-G', '-P', '*****'],
            BCP::maskCredentials(['bcp', '-G', '-P', '/tmp/mssql-sp-token-abc']),
        );
    }

    private function createBcp(MSSQLDatabaseConfig $config, string $token = 'token'): BCP
    {
        $connection = $this->createMock(MSSQLConnection::class);
        $connection->method('quoteIdentifier')
            ->willReturnCallback(fn(string $str): string => '[' . $str . ']');

        $tokenProvider = $this->createMock(ServicePrincipalTokenProvider::class);
        $tokenProvider->method('getAccessToken')->willReturn($token);

        return new BCP($connection, $config, new TestLogger(), $tokenProvider);
    }

    private function createConfig(): MSSQLDatabaseConfig
    {
        return new MSSQLDatabaseConfig(
            'mssql',
            '1433',
            'test',
            'sa',
            'password',
            null,
            null,
            null,
            null,
            null,
            null,
        );
    }

    private function createServicePrincipalConfig(
        ?string $tenantId = 'tenant-id',
        ?string $clientId = 'client-id',
        ?string $clientSecret = 'client-secret',
    ): MSSQLDatabaseConfig {
        return new MSSQLDatabaseConfig(
            'mssql',
            '1433',
            'test',
            '',
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            $tenantId,
            $clientId,
            $clientSecret,
        );
    }
}
