<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Query;

use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionHelper as BaseQueryExpressionHelper;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategyInterface;
use Override;

use function count;
use function is_array;
use function is_callable;
use function is_string;

/**
 * @phpstan-type QueryExpressionProviderOptions = array{
 *     getIndexMappingFn?: callable(): array<string, mixed>,
 *     fullTextSearchTerm?: string|null,
 * }
 */
class QueryExpressionProvider implements QueryExpressionProviderInterface
{
    /** @phpstan-var array<string, string> */
    private array $fieldMapping = [];

    /** @phpstan-var string|string[] */
    private array|string $rootAlias = [];

    /** @phpstan-var string|string[] */
    private array|string $rootIdentifier = [];

    public function __construct(
        private readonly ReadModelDescriptorFactoryInterface $descriptorFactory,
        private readonly QueryStrategyInterface $queryStrategy = new QueryStrategy9x(),
    ) {
    }

    #[Override]
    public function setFieldMapping(array $fieldMapping): self
    {
        $this->fieldMapping = $fieldMapping;

        return $this;
    }

    #[Override]
    public function getFieldMapping(): array
    {
        return $this->fieldMapping;
    }

    #[Override]
    public function setRootAlias(array|string $rootAlias): self
    {
        $this->rootAlias = $rootAlias;

        return $this;
    }

    #[Override]
    public function getRootAlias(): array|string
    {
        return $this->rootAlias;
    }

    #[Override]
    public function setRootIdentifier(array|string $rootIdentifier): self
    {
        $this->rootIdentifier = $rootIdentifier;

        return $this;
    }

    #[Override]
    public function getRootIdentifier(): array|string
    {
        return $this->rootIdentifier;
    }

    #[Override]
    public function requireSingleRootIdentifier(): string
    {
        return BaseQueryExpressionHelper::requireSingleValueRootIdentifier($this->rootIdentifier);
    }

    /**
     * Applies a QueryExpression to an Elasticsearch params array.
     *
     * The returned array contains `query` and optionally `sort` — pagination params are not added here.
     *
     * Required option: `getIndexMappingFn` — a callable returning the flattened Elasticsearch mapping.
     * It is called lazily only when filter, sort, or values are present.
     *
     * Optional options:
     *  - `fullTextSearchTerm` (string|null) — wraps any filter in a full-text search query
     *
     * @phpstan-param array<string, mixed> $data  initial params array (merged into the result)
     * @phpstan-param QueryExpressionProviderOptions $options
     *
     * @phpstan-return array<string, mixed>
     */
    #[Override]
    public function apply(mixed $data, QueryExpression $queryExpression, ReadModelDescriptor|null $descriptor = null, array $options = [], int $includeData = self::INCLUDE_DATA_ALL): array
    {
        $optDescriptor = $options['read_model_descriptor'] ?? null;
        if ($descriptor === null && is_string($optDescriptor)) {
            $optDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($optDescriptor);
        }

        if ($descriptor === null && $optDescriptor instanceof ReadModelDescriptor) {
            $descriptor = $optDescriptor;
        }

        $fieldMapping       = count($this->fieldMapping) > 0 ? $this->fieldMapping : ($descriptor->fieldMap ?? []);
        $fullTextSearchTerm = $options['fullTextSearchTerm'] ?? null;

        $identifierField = is_string($this->rootIdentifier) ? $this->rootIdentifier : ($this->rootIdentifier[0] ?? 'id');

        $hasContent = ($queryExpression->getFilter() !== null && ! $queryExpression->getFilter()->isFilterEmpty())
            || ($queryExpression->getSort() !== null && ! $queryExpression->getSort()->isSortEmpty())
            || ($queryExpression->getValues() !== null && count($queryExpression->getValues()) > 0);

        $getIndexMappingFn = $options['getIndexMappingFn'] ?? null;
        $indexMapping      = $hasContent && is_callable($getIndexMappingFn) ? $getIndexMappingFn() : [];

        $helper = QueryExpressionHelper::create($indexMapping, $this->queryStrategy);
        $result = $helper->apply($queryExpression, $fullTextSearchTerm, $identifierField, $fieldMapping, $includeData);

        return [...(is_array($data) ? $data : []), ...$result];
    }
}
