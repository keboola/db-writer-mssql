<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\NodeDefinition;

use Keboola\DbWriterConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class MSSQLDbNode extends DbNode
{
    public function init(NodeBuilder $nodeBuilder): void
    {
        parent::init($nodeBuilder);
        $this->addTdsVersion($nodeBuilder);
        $this->addInstance($nodeBuilder);
        $this->addCollation($nodeBuilder);
        $this->addServicePrincipalNodes($nodeBuilder);
        $this->addAuthValidation();
    }

    protected function addUserNode(NodeBuilder $builder): void
    {
        // Optional: not required when Service Principal credentials are provided.
        $builder->scalarNode('user');
    }

    protected function addPasswordNode(NodeBuilder $builder): void
    {
        $this->beforeNormalization()->always(function (array $v) {
            if (isset($v['password'])) {
                $v['#password'] = $v['password'];
                unset($v['password']);
            }
            return $v;
        });

        // Optional: not required when Service Principal credentials are provided.
        $builder->scalarNode('#password');
    }

    protected function addServicePrincipalNodes(NodeBuilder $nodeBuilder): void
    {
        // Microsoft Entra ID (Azure AD) Service Principal credentials
        $nodeBuilder->scalarNode('tenantId');
        $nodeBuilder->scalarNode('clientId');
        $nodeBuilder->scalarNode('#clientSecret');
    }

    protected function addAuthValidation(): void
    {
        $this->validate()->always(function (array $v): array {
            $spKeys = ['tenantId', 'clientId', '#clientSecret'];
            $spFilled = count(array_filter($spKeys, fn(string $k): bool => !empty($v[$k])));

            if ($spFilled > 0 && $spFilled < count($spKeys)) {
                throw new InvalidConfigurationException(
                    'For Service Principal authentication all of "tenantId", "clientId" and '
                    . '"#clientSecret" must be set.',
                );
            }

            if ($spFilled === 0 && (empty($v['user']) || empty($v['#password']))) {
                throw new InvalidConfigurationException(
                    'Either "user" and "#password" (SQL login) or "tenantId", "clientId" and '
                    . '"#clientSecret" (Service Principal) must be set.',
                );
            }

            return $v;
        });
    }

    private function addTdsVersion(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('tdsVersion')->defaultValue('7.1');
    }

    private function addInstance(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('instance');
    }

    private function addCollation(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder->scalarNode('collation');
    }

    protected function addSchemaNode(NodeBuilder $builder): void
    {
        $builder->scalarNode('schema');
    }
}
