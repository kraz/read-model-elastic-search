<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Query;

use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionHelper as BaseQueryExpressionHelper;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
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
 *     root_identifier?: string|string[],
 *     field_map?: array<string, string>,
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
        private readonly QueryStrategyInterface $queryStrategy,
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

    public function getQueryStrategy(): QueryStrategyInterface
    {
        return $this->queryStrategy;
    }

    /**
     * @phpstan-param array<string, mixed> $data
     * @phpstan-param QueryExpressionProviderOptions $options
     */
    #[Override]
    public function mapField(string $field, mixed $data = null, ReadModelDescriptor|null $descriptor = null, array $options = []): string
    {
        return $this->createHelper($descriptor, $options)->mapField($field);
    }

    /**
     * Applies a QueryExpression to an Elasticsearch params array.
     *
     * The returned array contains `query` and optionally `sort` — pagination params are not added here.
     *
     * Required option: `getIndexMappingFn` — a callable returning the flattened Elasticsearch mapping.
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
        $fullTextSearchTerm = $options['fullTextSearchTerm'] ?? null;

        $hasContent = ($queryExpression->getFilter() !== null && ! $queryExpression->getFilter()->isFilterEmpty())
            || ($queryExpression->getSort() !== null && ! $queryExpression->getSort()->isSortEmpty())
            || ($queryExpression->getValues() !== null && count($queryExpression->getValues()) > 0);

        $helper = $this->createHelper($descriptor, $options, $hasContent);
        $result = $helper->apply($queryExpression, $fullTextSearchTerm, $includeData);

        return [...(is_array($data) ? $data : []), ...$result];
    }

    /**
     * Build the Elasticsearch params for one cursor-paginated request.
     *
     * Delegates to {@see QueryExpressionHelper::applyCursor()} so all ES-specific
     * cursor syntax (search_after, track_total_hits, sort overrides) is kept inside
     * the helper/strategy and out of the DataSource.
     *
     * @phpstan-param list<mixed>|null               $searchAfter
     * @phpstan-param int<1, max>                    $size
     * @phpstan-param QueryExpressionProviderOptions $options
     *
     * @phpstan-return array<string, mixed>
     */
    public function applyCursor(
        QueryExpression $queryExpression,
        SortExpression $orderBySort,
        array|null $searchAfter,
        int $size,
        ReadModelDescriptor|null $descriptor = null,
        array $options = [],
    ): array {
        $fullTextSearchTerm = $options['fullTextSearchTerm'] ?? null;
        $helper             = $this->createHelper($descriptor, $options);

        return $helper->applyCursor($queryExpression, $fullTextSearchTerm, $orderBySort, $searchAfter, $size);
    }

    /** @phpstan-param QueryExpressionProviderOptions $options */
    private function createHelper(ReadModelDescriptor|null $descriptor = null, array $options = [], bool $loadIndexMapping = true): QueryExpressionHelper
    {
        $optDescriptor = $options['read_model_descriptor'] ?? null;
        if ($descriptor === null && is_string($optDescriptor)) {
            $optDescriptor = $this->descriptorFactory->createReadModelDescriptorFrom($optDescriptor);
        }

        if ($descriptor === null && $optDescriptor instanceof ReadModelDescriptor) {
            $descriptor = $optDescriptor;
        }

        $options['field_map']       ??= $descriptor ? $descriptor->fieldMap : $this->fieldMapping;
        $options['root_identifier'] ??= $this->rootIdentifier;

        $getIndexMappingFn = $options['getIndexMappingFn'] ?? null;
        $indexMapping      = $loadIndexMapping && is_callable($getIndexMappingFn) ? $getIndexMappingFn() : [];

        return QueryExpressionHelper::create($indexMapping, $this->queryStrategy, $options);
    }
}
