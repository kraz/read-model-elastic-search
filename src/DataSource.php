<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ReadModel\Pagination\InMemoryPaginator;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\ReadDataProviderAccess;
use Kraz\ReadModel\ReadDataProviderComposition;
use Kraz\ReadModel\ReadDataProviderCompositionInterface;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModel\ReadDataProviderPayload;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategyInterface;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Symfony\Bridge\PsrHttpMessage\Factory\PsrHttpFactory;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;

/**
 * @template T of array<string, mixed>|object
 *
 * @implements ReadDataProviderInterface<T>
 * @implements FullTextSearchReadModelInterface<T>
 * @implements ElasticRawQuerySearchReadModelInterface<T>
 */
class DataSource implements ReadDataProviderInterface, FullTextSearchReadModelInterface, ElasticRawQuerySearchReadModelInterface
{
    /** @use ReadDataProviderAccess<T> */
    use ReadDataProviderAccess;
    /** @use ReadDataProviderComposition<T> */
    use ReadDataProviderComposition;

    /**
     * @var ReadDataProviderPayload<T>|null
     */
    private ?ReadDataProviderPayload $payload = null;
    private ?PaginatorInterface $paginatorInstance = null;
    private ?string $fullTextSearchTerm = null;
    private ?string $rawQuerySearchPayload = null;

    /**
     * @param ElasticSearchClientInterface&ElasticSearchReadClientInterface<T> $client
     */
    public function __construct(
        private readonly ElasticSearchClientInterface&ElasticSearchReadClientInterface $client,
        private readonly string $identifierField = 'id',
        private readonly ?string $index = null,
        private readonly QueryStrategyInterface $queryStrategy = new QueryStrategy9x(),
    ) {
    }

    /**
     * @return ReadDataProviderPayload<T>
     */
    private function getPayload(): ReadDataProviderPayload
    {
        if (null !== $this->payload) {
            return $this->payload;
        }

        $result = $this->client->read($this->getParams(), $this->index);

        /** @psalm-suppress ArgumentTypeCoercion */
        /** @var ReadDataProviderPayload<T> $payload */
        $payload = new ReadDataProviderPayload($result);
        $this->payload = $payload;

        return $this->payload;
    }

    private function getWrappedQueryExpression(): ?QueryExpression
    {
        if (0 === \count($this->queryExpressions)) {
            return null;
        }

        if (1 === \count($this->queryExpressions)) {
            return clone $this->queryExpressions[0];
        }

        return array_reduce($this->queryExpressions, static fn (QueryExpression $qx, QueryExpression $item) => $qx->wrap($item), QueryExpression::create());
    }

    private function getParams(): array
    {
        if (null !== $this->rawQuerySearchPayload) {
            return json_decode($this->rawQuerySearchPayload, true);
        }

        $params = [];

        $queryExpression = $this->getWrappedQueryExpression()?->toArray() ?? [];
        $fieldMapping = $this->getOrCreateQueryExpressionProvider()->getFieldMapping();
        if (\count($queryExpression) > 0 && \count($fieldMapping) > 0) {
            $queryExpression = QueryExpression::applyFieldMapping($queryExpression, $fieldMapping);
        }
        $filter = $queryExpression['filters'] ?? $queryExpression['filter'] ?? null;
        $sort = $queryExpression['sort'] ?? null;
        $values = $queryExpression['values'] ?? null;

        $elasticMapping = null;
        $filterQuery = null;

        if (\is_array($filter)) {
            $filter = FilterExpression::walkFieldValues($filter, fn ($field, $value) => $this->escapeQueryString($value));
            $elasticMapping ??= $this->client->getFlattenedMapping($this->index, $this->queryStrategy->extractMappingProperties(...));
            $filterQuery = $this->buildElasticQuery($filter, $elasticMapping);
        }

        if (null !== $this->fullTextSearchTerm) {
            $params['query'] = $this->queryStrategy->buildFullTextSearchWithFilter(
                $this->escapeQueryString($this->fullTextSearchTerm),
                $filterQuery,
            );
        } elseif (\is_array($filterQuery)) {
            $params['query'] = $filterQuery;
        }

        if (\is_array($sort)) {
            $elasticMapping ??= $this->client->getFlattenedMapping($this->index, $this->queryStrategy->extractMappingProperties(...));
            $params['sort'] = $this->buildElasticSort($sort, $elasticMapping);
        }

        if (\is_array($values)) {
            $valuesQuery = FilterExpression::create()
                ->inList($fieldMapping[$this->identifierField] ?? $this->identifierField, array_map($this->escapeQueryString(...), $values))->toArray();
            $elasticMapping ??= $this->client->getFlattenedMapping($this->index, $this->queryStrategy->extractMappingProperties(...));
            $params['query'] = $this->buildElasticQuery($valuesQuery, $elasticMapping);
        }

        if (!isset($params['query'])) {
            $params['query'] = ['match_all' => new \stdClass()];
        }
        if ($this->isPaginated()) {
            [$page, $itemsPerPage] = $this->pagination;
            $params['from'] = ($page - 1) * $itemsPerPage;
            $params['size'] = $itemsPerPage;
        } elseif (null !== $this->limit) {
            [$limitValue, $offsetValue] = $this->limit;
            $params['size'] = $limitValue;
            if (null !== $offsetValue && $offsetValue > 0) {
                $params['from'] = $offsetValue;
            }
        }

        return $params;
    }

    private function buildElasticSort(array $sort, array $mapping): array
    {
        if (empty($sort)) {
            return [];
        }

        $elasticSort = [];

        foreach ($sort as $sortItem) {
            $field = $sortItem['field'] ?? null;
            $direction = $sortItem['dir'] ?? 'asc';

            if (null === $field) {
                continue;
            }

            // Normalize direction
            $direction = mb_strtolower($direction);
            if (!\in_array($direction, ['asc', 'desc'], true)) {
                $direction = 'asc';
            }

            // Get field information from mapping
            $fieldInfo = $this->getFieldInfo($field, $mapping);
            $fieldType = $fieldInfo['type'];
            $nestedPath = $fieldInfo['nestedPath'];

            if (null !== $nestedPath) {
                $elasticSort[] = $this->buildNestedSort($field, $direction, $nestedPath, $fieldType);
            } else {
                $elasticSort[] = $this->buildRegularSort($field, $direction, $fieldType);
            }
        }

        return $elasticSort;
    }

    private function buildNestedSort(string $field, string $direction, string $nestedPath, ?string $fieldType): array
    {
        $sortField = $this->queryStrategy->getSortableField($field, $fieldType);
        $unmappedType = $this->queryStrategy->getUnmappedType($fieldType);

        return [
            $sortField => [
                'order' => $direction,
                'missing' => 'desc' === $direction ? '_first' : '_last',
                'nested' => [
                    'path' => $nestedPath,
                ],
                'unmapped_type' => $unmappedType,
            ],
        ];
    }

    private function buildRegularSort(string $field, string $direction, ?string $fieldType): array
    {
        $sortField = $this->queryStrategy->getSortableField($field, $fieldType);
        $unmappedType = $this->queryStrategy->getUnmappedType($fieldType);

        return [
            $sortField => [
                'order' => $direction,
                'missing' => 'desc' === $direction ? '_first' : '_last',
                'unmapped_type' => $unmappedType,
            ],
        ];
    }

    private function buildElasticQuery(array $filter, array $mapping): array
    {
        if (empty($filter)) {
            return ['match_all' => new \stdClass()];
        }

        return $this->convertFilterToElasticQuery($filter, $mapping);
    }

    private function convertFilterToElasticQuery(array $filter, array $mapping): array
    {
        // Check if this is a simple filter (has field/operator/value) or complex filter (has filters/logic)
        if (isset($filter['field']) && isset($filter['operator'])) {
            return $this->buildSimpleFilter($filter, $mapping);
        }

        if (isset($filter['filters']) && \is_array($filter['filters'])) {
            return $this->buildComplexFilter($filter, $mapping);
        }

        // If neither structure is matched, return match_all
        return ['match_all' => new \stdClass()];
    }

    private function buildSimpleFilter(array $filter, array $mapping): array
    {
        $field = $filter['field'];
        $operator = $filter['operator'];
        $value = $filter['value'] ?? null;
        $ignoreCase = $filter['ignoreCase'] ?? true;
        $not = $filter['not'] ?? false;

        // Get the ElasticSearch field type and nested path from mapping if available
        $fieldInfo = $this->getFieldInfo($field, $mapping);
        $fieldType = $fieldInfo['type'];
        $nestedPath = $fieldInfo['nestedPath'];

        $query = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldType);

        // Handle negation
        if ($not) {
            $query = ['bool' => ['must_not' => [$query]]];
        }

        // Wrap in nested query if field is in a nested object
        if (null !== $nestedPath) {
            $query = ['nested' => ['path' => $nestedPath, 'query' => $query]];
        }

        return $query;
    }

    private function buildComplexFilter(array $filter, array $mapping): array
    {
        $logic = $filter['logic'] ?? 'and';
        $filters = $filter['filters'] ?? [];
        $not = $filter['not'] ?? false;

        if (empty($filters)) {
            return ['match_all' => new \stdClass()];
        }

        // Group filters by nested path to optimize nested queries
        $nestedGroups = [];
        $regularQueries = [];

        foreach ($filters as $subFilter) {
            $nestedPath = $this->getFilterNestedPath($subFilter, $mapping);

            if (null !== $nestedPath) {
                // Group nested filters by their path
                if (!isset($nestedGroups[$nestedPath])) {
                    $nestedGroups[$nestedPath] = [];
                }
                $nestedGroups[$nestedPath][] = $subFilter;
            } else {
                // Regular (non-nested) filter
                $subQuery = $this->convertFilterToElasticQuery($subFilter, $mapping);
                if (!empty($subQuery)) {
                    $regularQueries[] = $subQuery;
                }
            }
        }

        // Build nested queries for each nested path
        foreach ($nestedGroups as $nestedPath => $nestedFilters) {
            if (1 === \count($nestedFilters)) {
                // Single nested filter
                $nestedQuery = $this->convertFilterToElasticQuery($nestedFilters[0], $mapping);
            } else {
                // Multiple nested filters - combine them
                $nestedSubQueries = [];
                foreach ($nestedFilters as $nestedFilter) {
                    // Build query without nested wrapper (will be added later)
                    if (isset($nestedFilter['field'])) {
                        $field = $nestedFilter['field'];
                        $operator = $nestedFilter['operator'];
                        $value = $nestedFilter['value'] ?? null;
                        $ignoreCase = $nestedFilter['ignoreCase'] ?? true;
                        $fieldInfo = $this->getFieldInfo($field, $mapping);
                        $nestedSubQuery = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldInfo['type']);

                        if ($nestedFilter['not'] ?? false) {
                            $nestedSubQuery = ['bool' => ['must_not' => [$nestedSubQuery]]];
                        }

                        $nestedSubQueries[] = $nestedSubQuery;
                    } else {
                        // Recursive nested filter - handle without nested wrapper initially
                        $nestedSubQuery = $this->buildComplexFilterWithoutNesting($nestedFilter, $mapping);
                        $nestedSubQueries[] = $nestedSubQuery;
                    }
                }

                // Combine nested queries
                if ('or' === $logic) {
                    $nestedQuery = ['nested' => [
                        'path' => $nestedPath,
                        'query' => ['bool' => ['should' => $nestedSubQueries, 'minimum_should_match' => 1]],
                    ]];
                } else {
                    $nestedQuery = ['nested' => [
                        'path' => $nestedPath,
                        'query' => ['bool' => ['must' => $nestedSubQueries]],
                    ]];
                }
            }

            $regularQueries[] = $nestedQuery;
        }

        if (empty($regularQueries)) {
            return ['match_all' => new \stdClass()];
        }

        if (1 === \count($regularQueries)) {
            $query = $regularQueries[0];
        } else {
            // Combine queries based on logic
            if ('or' === $logic) {
                $query = ['bool' => ['should' => $regularQueries, 'minimum_should_match' => 1]];
            } else { // 'and' or default
                $query = ['bool' => ['must' => $regularQueries]];
            }
        }

        // Handle negation
        if ($not) {
            $query = ['bool' => ['must_not' => [$query]]];
        }

        return $query;
    }

    private function buildComplexFilterWithoutNesting(array $filter, array $mapping): array
    {
        $logic = $filter['logic'] ?? 'and';
        $filters = $filter['filters'] ?? [];

        if (empty($filters)) {
            return ['match_all' => new \stdClass()];
        }

        $subQueries = [];
        foreach ($filters as $subFilter) {
            if (isset($subFilter['field'])) {
                $field = $subFilter['field'];
                $operator = $subFilter['operator'];
                $value = $subFilter['value'] ?? null;
                $ignoreCase = $subFilter['ignoreCase'] ?? true;
                $fieldInfo = $this->getFieldInfo($field, $mapping);
                $subQuery = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldInfo['type']);

                if ($subFilter['not'] ?? false) {
                    $subQuery = ['bool' => ['must_not' => [$subQuery]]];
                }

                $subQueries[] = $subQuery;
            } else {
                $subQuery = $this->buildComplexFilterWithoutNesting($subFilter, $mapping);
                $subQueries[] = $subQuery;
            }
        }

        if (empty($subQueries)) {
            return ['match_all' => new \stdClass()];
        }

        if (1 === \count($subQueries)) {
            return $subQueries[0];
        }

        // Combine queries based on logic
        if ('or' === $logic) {
            return ['bool' => ['should' => $subQueries, 'minimum_should_match' => 1]];
        }

        return ['bool' => ['must' => $subQueries]];
    }

    private function getFilterNestedPath(array $filter, array $mapping): ?string
    {
        // Check if this is a simple filter with a field
        if (isset($filter['field'])) {
            $fieldInfo = $this->getFieldInfo($filter['field'], $mapping);

            return $fieldInfo['nestedPath'];
        }

        // Check if this is a complex filter - get nested path from first field found
        if (isset($filter['filters']) && \is_array($filter['filters'])) {
            foreach ($filter['filters'] as $subFilter) {
                $nestedPath = $this->getFilterNestedPath($subFilter, $mapping);
                if (null !== $nestedPath) {
                    return $nestedPath;
                }
            }
        }

        return null;
    }

    private function buildElasticQueryForOperator(string $field, string $operator, mixed $value, bool $ignoreCase, ?string $fieldType): array
    {
        switch ($operator) {
            case FilterExpression::OP_EQ:
                if ($ignoreCase && 'keyword' !== $fieldType) {
                    return ['match' => [$field => ['query' => $value, 'operator' => 'and']]];
                }

                return ['term' => [$field => $value]];

            case FilterExpression::OP_NEQ:
                $termQuery = $ignoreCase && 'keyword' !== $fieldType
                    ? ['match' => [$field => ['query' => $value, 'operator' => 'and']]]
                    : ['term' => [$field => $value]];

                return ['bool' => ['must_not' => [$termQuery]]];

            case FilterExpression::OP_IS_NULL:
                return ['bool' => ['must_not' => [['exists' => ['field' => $field]]]]];

            case FilterExpression::OP_IS_NOT_NULL:
                return ['exists' => ['field' => $field]];

            case FilterExpression::OP_LT:
                return ['range' => [$field => ['lt' => $value]]];

            case FilterExpression::OP_LTE:
                return ['range' => [$field => ['lte' => $value]]];

            case FilterExpression::OP_GT:
                return ['range' => [$field => ['gt' => $value]]];

            case FilterExpression::OP_GTE:
                return ['range' => [$field => ['gte' => $value]]];

            case FilterExpression::OP_STARTS_WITH:
                if ($ignoreCase) {
                    return ['wildcard' => [$field => ['value' => mb_strtolower($value).'*', 'case_insensitive' => true]]];
                }

                return ['prefix' => [$field => $value]];

            case FilterExpression::OP_DOES_NOT_START_WITH:
                $prefixQuery = $ignoreCase
                    ? ['wildcard' => [$field => ['value' => mb_strtolower($value).'*', 'case_insensitive' => true]]]
                    : ['prefix' => [$field => $value]];

                return ['bool' => ['must_not' => [$prefixQuery]]];

            case FilterExpression::OP_ENDS_WITH:
                $pattern = $ignoreCase ? '*'.mb_strtolower($value) : '*'.$value;
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return $wildcardQuery;

            case FilterExpression::OP_DOES_NOT_END_WITH:
                $pattern = $ignoreCase ? '*'.mb_strtolower($value) : '*'.$value;
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return ['bool' => ['must_not' => [$wildcardQuery]]];

            case FilterExpression::OP_CONTAINS:
                $pattern = $ignoreCase ? '*'.mb_strtolower($value).'*' : '*'.$value.'*';
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return $wildcardQuery;

            case FilterExpression::OP_DOES_NOT_CONTAIN:
                $pattern = $ignoreCase ? '*'.mb_strtolower($value).'*' : '*'.$value.'*';
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return ['bool' => ['must_not' => [$wildcardQuery]]];

            case FilterExpression::OP_IS_EMPTY:
                return ['bool' => [
                    'should' => [
                        ['bool' => ['must_not' => [['exists' => ['field' => $field]]]]],
                        ['term' => [$field => '']],
                    ],
                    'minimum_should_match' => 1,
                ]];

            case FilterExpression::OP_IS_NOT_EMPTY:
                return ['bool' => [
                    'must' => [['exists' => ['field' => $field]]],
                    'must_not' => [['term' => [$field => '']]],
                ]];

            case FilterExpression::OP_IN_LIST:
                if (!\is_array($value)) {
                    $value = [$value];
                }

                // For text fields, use bool/should with match queries instead of terms
                // because terms query doesn't work with analyzed text fields
                if ('text' === $fieldType) {
                    $shouldQueries = [];
                    foreach ($value as $item) {
                        if ($ignoreCase) {
                            $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                        } else {
                            // For case-sensitive, try .keyword subfield first, then fallback to match
                            $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                        }
                    }

                    return ['bool' => ['should' => $shouldQueries, 'minimum_should_match' => 1]];
                }

                // For keyword, numeric, and other non-analyzed fields, use terms query
                return ['terms' => [$field => $value]];

            case FilterExpression::OP_NOT_IN_LIST:
                if (!\is_array($value)) {
                    $value = [$value];
                }

                // For text fields, use bool/must_not with should/match queries
                if ('text' === $fieldType) {
                    $shouldQueries = [];
                    foreach ($value as $item) {
                        if ($ignoreCase) {
                            $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                        } else {
                            $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                        }
                    }

                    return ['bool' => ['must_not' => [['bool' => ['should' => $shouldQueries, 'minimum_should_match' => 1]]]]];
                }

                // For keyword, numeric, and other non-analyzed fields, use terms query
                return ['bool' => ['must_not' => [['terms' => [$field => $value]]]]];

            default:
                // Fallback to term query for unknown operators
                return ['term' => [$field => $value]];
        }
    }

    private function getFieldInfo(string $field, array $mapping): array
    {
        // Try to get field info from flattened ElasticSearch mapping
        if (isset($mapping[$field])) {
            return [
                'type' => $mapping[$field]['type'] ?? null,
                'nestedPath' => $mapping[$field]['nestedPath'] ?? null,
            ];
        }

        // Fallback: try to find in complex mapping structure
        $fieldParts = explode('.', $field);
        $currentMapping = $mapping;

        foreach ($fieldParts as $part) {
            if (isset($currentMapping[$part])) {
                $currentMapping = $currentMapping[$part];
                if (isset($currentMapping['type'])) {
                    return [
                        'type' => $currentMapping['type'],
                        'nestedPath' => $currentMapping['nestedPath'] ?? null,
                    ];
                }
                if (isset($currentMapping['properties'])) {
                    $currentMapping = $currentMapping['properties'];
                }
            } else {
                break;
            }
        }

        return ['type' => null, 'nestedPath' => null];
    }

    private function escapeQueryString(string $value): string
    {
        return $value;
    }

    #[\Override]
    public function withQueryModifier(callable $modifier, bool $append = false): static
    {
        throw new \LogicException('Query modifiers are not supported in the ElasticSearch DataSource.');
    }

    /**
     * @return array<int, T>
     */
    private function filteredItems(): array
    {
        $items = $this->getPayload()->getData();

        if (0 === \count($this->specifications)) {
            return $items;
        }

        return array_values(array_filter($items, function (mixed $item): bool {
            foreach ($this->specifications as $specification) {
                /** @var T $item */
                if (!$specification->isSatisfiedBy($item)) {
                    return false;
                }
            }

            return true;
        }));
    }

    #[\Override]
    public function count(): int
    {
        $this->assertNoSpecifications();

        if (null !== $paginator = $this->paginator()) {
            return $paginator->count();
        }

        return \count($this->filteredItems());
    }

    #[\Override]
    public function totalCount(): int
    {
        $this->assertNoSpecifications();

        return $this->getPayload()->getTotalItems();
    }

    #[\Override]
    public function isPaginated(): bool
    {
        if (null === $this->pagination) {
            return false;
        }

        [$page, $itemsPerPage] = $this->pagination;

        return $page > 0 && $itemsPerPage > 0;
    }

    #[\Override]
    public function isEmpty(): bool
    {
        $this->assertNoSpecifications();

        return 0 === $this->totalCount();
    }

    #[\Override]
    public function getIterator(): \Traversable
    {
        $specifications = $this->specifications;
        $hasSpecs = \count($specifications) > 0;

        if ($hasSpecs && null !== $this->limit) {
            [$limitValue, $offsetValue] = $this->limit;
            yield from $this->withoutSpecification()->withoutLimit()->specificationsIterator(
                $specifications,
                $limitValue,
                $offsetValue ?? 0,
            );

            return;
        }

        if ($hasSpecs) {
            $items = new \ArrayIterator($this->filteredItems());
        } elseif (null !== $paginator = $this->paginator()) {
            $items = $paginator->getIterator();
        } else {
            $items = new \ArrayIterator($this->filteredItems());
        }

        $itemNormalizer = $this->itemNormalizer;
        if (null !== $itemNormalizer) {
            foreach ($items as $item) {
                yield $itemNormalizer($item);
            }

            return;
        }

        yield from $items;
    }

    #[\Override]
    public function data(): array
    {
        return iterator_to_array($this->getIterator());
    }

    #[\Override]
    public function getResult(): array|ReadResponse
    {
        $this->assertNoSpecifications();

        if ($this->isValue()) {
            return $this->data();
        }

        $data = $this->data();

        return ReadResponse::create(
            $data,
            $this->isPaginated() ? ($this->paginator()?->getCurrentPage() ?? 1) : 1,
            $this->totalCount(),
        );
    }

    #[\Override]
    public function paginator(): ?PaginatorInterface
    {
        $this->assertNoSpecifications();

        if (!$this->isPaginated() || $this->isValue()) {
            return null;
        }

        if (null === $this->paginatorInstance) {
            [$page, $itemsPerPage] = $this->pagination;
            $payload = $this->getPayload();
            $iterator = \count($this->specifications) > 0
                ? new \ArrayIterator($this->filteredItems())
                : $payload->getIterator();
            $this->paginatorInstance = new InMemoryPaginator(
                $iterator,
                $payload->getTotalItems(),
                $payload->getCurrentPage() ?: $page,
                $itemsPerPage,
                0
            );
        }

        return $this->paginatorInstance;
    }

    #[\Override]
    public function withFullTextSearch(string $term): static
    {
        /** @phpstan-var static<T> $clone */
        $clone = clone $this;
        $clone->fullTextSearchTerm = $term;

        return $clone;
    }

    #[\Override]
    public function withoutFullTextSearch(): static
    {
        /** @phpstan-var static<T> $clone */
        $clone = clone $this;
        $clone->fullTextSearchTerm = null;

        return $clone;
    }

    #[\Override]
    public function withRawQuerySearch(string $query): static
    {
        /** @phpstan-var static<T> $clone */
        $clone = $this
            ->withoutSpecification()
            ->withoutQueryExpression()
            ->withoutPagination()
            ->withoutFullTextSearch();

        $clone->rawQuerySearchPayload = $query;

        return $clone;
    }

    #[\Override]
    public function withoutRawQuerySearch(): static
    {
        /** @phpstan-var static<T> $clone */
        $clone = clone $this;
        $clone->rawQuerySearchPayload = null;

        return $clone;
    }

    private function assertNoSpecifications(): void
    {
        if (\count($this->specifications) > 0) {
            throw new \LogicException('Cannot use this method when specifications are set. Use getIterator() or data() instead.');
        }
    }

    #[\Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        /** @phpstan-var static<T> $ds */
        $ds = static::applyRequestTo($this, $request, $fieldsOperator, $fieldsIgnoreCase);

        return $ds;
    }

    public function __clone()
    {
        $this->payload = null;
        $this->paginatorInstance = null;
    }

    /**
     * @phpstan-param ReadDataProviderCompositionInterface $target
     * @phpstan-param array<string, string> $fieldsOperator
     * @phpstan-param array<string, bool> $fieldsIgnoreCase
     *
     * @phpstan-return ReadDataProviderCompositionInterface
     *
     * @phpstan-template J of ReadDataProviderCompositionInterface<object|array<string, mixed>>
     */
    public static function applyRequestTo(ReadDataProviderCompositionInterface $target, object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): ReadDataProviderCompositionInterface
    {
        if (class_exists(SymfonyRequest::class) && $request instanceof SymfonyRequest) {
            if (!class_exists(Psr17Factory::class)) {
                throw new \InvalidArgumentException('You need to install "nyholm/psr7" and "symfony/psr-http-message-bridge" in order to handle Symfony requests!');
            }

            $psr17Factory = new Psr17Factory();
            $psrHttpFactory = new PsrHttpFactory($psr17Factory, $psr17Factory, $psr17Factory, $psr17Factory);
            $request = $psrHttpFactory->createRequest($request);
        }

        if ($request instanceof RequestInterface) {
            parse_str($request->getUri()->getQuery(), $input);

            /**
             * @phpstan-var ReadDataProviderCompositionInterface $result
             *
             * @phpstan-ignore argument.type
             */
            $result = $target->handleInput($input, $fieldsOperator, $fieldsIgnoreCase);

            return $result;
        }

        throw new \RuntimeException(\sprintf('Unsupported request type: %s', $request::class));
    }
}
