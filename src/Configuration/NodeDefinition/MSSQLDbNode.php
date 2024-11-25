<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\NodeDefinition;

use Keboola\DbWriterConfig\Configuration\NodeDefinition\DbNode;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;

class MSSQLDbNode extends DbNode
{
    public function init(NodeBuilder $nodeBuilder): void
    {
        parent::init($nodeBuilder);
        $this->addTdsVersion($nodeBuilder);
        $this->addInstance($nodeBuilder);
        $this->addCollation($nodeBuilder);
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
        $builder->scalarNode('schema')->defaultValue('dbo');
    }
}
