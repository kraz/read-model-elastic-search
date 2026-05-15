<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Query;

use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategyInterface;
use stdClass;

use function array_map;
use function count;
use function explode;
use function in_array;
use function is_array;
use function is_string;
use function mb_strtolower;

/**
 * @phpstan-type QueryExpressionHelperOptions = array{
 *     root_identifier?: string|string[],
 *     field_map?: array<string, string>,
 * }
 */
final class QueryExpressionHelper
{
    /**
     * @phpstan-param array<string, mixed> $indexMapping
     * @phpstan-param QueryExpressionHelperOptions $options
     */
    private function __construct(
        private readonly array $indexMapping,
        private readonly QueryStrategyInterface $queryStrategy,
        private readonly array $options,
    ) {
    }

    /**
     * @phpstan-param array<string, mixed> $indexMapping
     * @phpstan-param QueryExpressionHelperOptions $options
     */
    public static function create(array $indexMapping, QueryStrategyInterface $queryStrategy, array $options): self
    {
        return new self($indexMapping, $queryStrategy, $options);
    }

    /** @phpstan-return array<string, string> */
    private function getFieldMapping(): array
    {
        return $this->options['field_map'] ?? [];
    }

    private function getRootIdentifier(): string
    {
        $identifierField = $this->options['root_identifier'] ?? 'id';

        return is_string($identifierField) ? $identifierField : ($identifierField[0] ?? 'id');
    }

    public function mapField(string $field): string
    {
        $fieldMapping = $this->getFieldMapping();

        return $fieldMapping[$field] ?? $field;
    }

    /**
     * Builds Elasticsearch params (query and sort) from a QueryExpression.
     * Pagination params (from/size) are not included — the caller adds them.
     *
     * @phpstan-return array<string, mixed>
     */
    public function apply(
        QueryExpression $queryExpression,
        string|null $fullTextSearchTerm,
        int $includeData,
    ): array {
        $includeFilter = (bool) ($includeData & QueryExpressionProviderInterface::INCLUDE_DATA_FILTER);
        $includeSort   = (bool) ($includeData & QueryExpressionProviderInterface::INCLUDE_DATA_SORT);
        $includeValues = (bool) ($includeData & QueryExpressionProviderInterface::INCLUDE_DATA_VALUES);

        $params          = [];
        $filterQuery     = null;
        $fieldMapping    = $this->getFieldMapping();
        $identifierField = $this->getRootIdentifier();

        $filter = $queryExpression->getFilter();
        if ($includeFilter && $filter !== null && ! $filter->isFilterEmpty()) {
            $filterArray = $filter->toArray();
            if (count($fieldMapping) > 0) {
                $filterArray = FilterExpression::applyFieldMapping($filterArray, $fieldMapping);
            }

            $filterArray = FilterExpression::walkFieldValues($filterArray, fn ($field, $value) => is_string($value) ? $this->escapeQueryString($value) : $value);
            $filterQuery = $this->buildElasticQuery($filterArray, $this->indexMapping);
        }

        if ($fullTextSearchTerm !== null) {
            $params['query'] = $this->queryStrategy->buildFullTextSearchWithFilter(
                $this->escapeQueryString($fullTextSearchTerm),
                $filterQuery,
            );
        } elseif (is_array($filterQuery)) {
            $params['query'] = $filterQuery;
        }

        $sort = $queryExpression->getSort();
        if ($includeSort && $sort !== null && ! $sort->isSortEmpty()) {
            $sortArray = $sort->toArray();
            if (count($fieldMapping) > 0) {
                $sortArray = SortExpression::applyFieldMapping($sortArray, $fieldMapping);
            }

            $params['sort'] = $this->buildElasticSort($sortArray, $this->indexMapping);
        }

        $values = $queryExpression->getValues();
        if ($includeValues && $values !== null && count($values) > 0) {
            $mappedIdentifier = $fieldMapping[$identifierField] ?? $identifierField;
            $valuesFilter     = FilterExpression::create()
                ->inList($mappedIdentifier, array_map(fn (int|string $v) => $this->escapeQueryString((string) $v), $values))
                ->toArray();
            $params['query']  = $this->buildElasticQuery($valuesFilter, $this->indexMapping);
        }

        if (! isset($params['query'])) {
            $params['query'] = ['match_all' => new stdClass()];
        }

        return $params;
    }

    private function escapeQueryString(string $value): string
    {
        return $value;
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function buildElasticSort(array $sort, array $indexMapping): array
    {
        if (empty($sort)) {
            return [];
        }

        $elasticSort = [];

        foreach ($sort as $sortItem) {
            $field     = $sortItem['field'] ?? null;
            $direction = $sortItem['dir'] ?? 'asc';

            if ($field === null) {
                continue;
            }

            $direction = mb_strtolower($direction);
            if (! in_array($direction, ['asc', 'desc'], true)) {
                $direction = 'asc';
            }

            $fieldInfo  = $this->getFieldInfo($field, $indexMapping);
            $fieldType  = $fieldInfo['type'];
            $nestedPath = $fieldInfo['nestedPath'];

            if ($nestedPath !== null) {
                $elasticSort[] = $this->buildNestedSort($field, $direction, $nestedPath, $fieldType);
            } else {
                $elasticSort[] = $this->buildRegularSort($field, $direction, $fieldType);
            }
        }

        return $elasticSort;
    }

    /** @phpstan-ignore missingType.iterableValue */
    private function buildNestedSort(string $field, string $direction, string $nestedPath, string|null $fieldType): array
    {
        $sortField    = $this->queryStrategy->getSortableField($field, $fieldType);
        $unmappedType = $this->queryStrategy->getUnmappedType($fieldType);

        return [
            $sortField => [
                'order' => $direction,
                'missing' => $direction === 'desc' ? '_first' : '_last',
                'nested' => ['path' => $nestedPath],
                'unmapped_type' => $unmappedType,
            ],
        ];
    }

    /** @phpstan-ignore missingType.iterableValue */
    private function buildRegularSort(string $field, string $direction, string|null $fieldType): array
    {
        $sortField    = $this->queryStrategy->getSortableField($field, $fieldType);
        $unmappedType = $this->queryStrategy->getUnmappedType($fieldType);

        return [
            $sortField => [
                'order' => $direction,
                'missing' => $direction === 'desc' ? '_first' : '_last',
                'unmapped_type' => $unmappedType,
            ],
        ];
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function buildElasticQuery(array $filter, array $indexMapping): array
    {
        if (empty($filter)) {
            return ['match_all' => new stdClass()];
        }

        return $this->convertFilterToElasticQuery($filter, $indexMapping);
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function convertFilterToElasticQuery(array $filter, array $indexMapping): array
    {
        if (isset($filter['field']) && isset($filter['operator'])) {
            return $this->buildSimpleFilter($filter, $indexMapping);
        }

        if (isset($filter['filters']) && is_array($filter['filters'])) {
            return $this->buildComplexFilter($filter, $indexMapping);
        }

        return ['match_all' => new stdClass()];
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function buildSimpleFilter(array $filter, array $indexMapping): array
    {
        $field      = $filter['field'];
        $operator   = $filter['operator'];
        $value      = $filter['value'] ?? null;
        $ignoreCase = $filter['ignoreCase'] ?? true;
        $not        = $filter['not'] ?? false;

        $fieldInfo  = $this->getFieldInfo($field, $indexMapping);
        $fieldType  = $fieldInfo['type'];
        $nestedPath = $fieldInfo['nestedPath'];

        $query = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldType);

        if ($not) {
            $query = ['bool' => ['must_not' => [$query]]];
        }

        if ($nestedPath !== null) {
            $query = ['nested' => ['path' => $nestedPath, 'query' => $query]];
        }

        return $query;
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function buildComplexFilter(array $filter, array $indexMapping): array
    {
        $logic   = $filter['logic'] ?? 'and';
        $filters = $filter['filters'] ?? [];
        $not     = $filter['not'] ?? false;

        if (empty($filters)) {
            return ['match_all' => new stdClass()];
        }

        $nestedGroups   = [];
        $regularQueries = [];

        foreach ($filters as $subFilter) {
            $nestedPath = $this->getFilterNestedPath($subFilter, $indexMapping);

            if ($nestedPath !== null) {
                if (! isset($nestedGroups[$nestedPath])) {
                    $nestedGroups[$nestedPath] = [];
                }

                $nestedGroups[$nestedPath][] = $subFilter;
            } else {
                $subQuery = $this->convertFilterToElasticQuery($subFilter, $indexMapping);
                if (! empty($subQuery)) {
                    $regularQueries[] = $subQuery;
                }
            }
        }

        foreach ($nestedGroups as $nestedPath => $nestedFilters) {
            if (count($nestedFilters) === 1) {
                $nestedQuery = $this->convertFilterToElasticQuery($nestedFilters[0], $indexMapping);
            } else {
                $nestedSubQueries = [];
                foreach ($nestedFilters as $nestedFilter) {
                    if (isset($nestedFilter['field'])) {
                        $field          = $nestedFilter['field'];
                        $operator       = $nestedFilter['operator'];
                        $value          = $nestedFilter['value'] ?? null;
                        $ignoreCase     = $nestedFilter['ignoreCase'] ?? true;
                        $fieldInfo      = $this->getFieldInfo($field, $indexMapping);
                        $nestedSubQuery = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldInfo['type']);

                        if ($nestedFilter['not'] ?? false) {
                            $nestedSubQuery = ['bool' => ['must_not' => [$nestedSubQuery]]];
                        }

                        $nestedSubQueries[] = $nestedSubQuery;
                    } else {
                        $nestedSubQueries[] = $this->buildComplexFilterWithoutNesting($nestedFilter, $indexMapping);
                    }
                }

                if ($logic === 'or') {
                    $nestedQuery = [
                        'nested' => [
                            'path' => $nestedPath,
                            'query' => ['bool' => ['should' => $nestedSubQueries, 'minimum_should_match' => 1]],
                        ],
                    ];
                } else {
                    $nestedQuery = [
                        'nested' => [
                            'path' => $nestedPath,
                            'query' => ['bool' => ['must' => $nestedSubQueries]],
                        ],
                    ];
                }
            }

            $regularQueries[] = $nestedQuery;
        }

        if (empty($regularQueries)) {
            return ['match_all' => new stdClass()];
        }

        if (count($regularQueries) === 1) {
            $query = $regularQueries[0];
        } elseif ($logic === 'or') {
            $query = ['bool' => ['should' => $regularQueries, 'minimum_should_match' => 1]];
        } else {
            $query = ['bool' => ['must' => $regularQueries]];
        }

        if ($not) {
            $query = ['bool' => ['must_not' => [$query]]];
        }

        return $query;
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue, missingType.iterableValue */
    private function buildComplexFilterWithoutNesting(array $filter, array $indexMapping): array
    {
        $logic   = $filter['logic'] ?? 'and';
        $filters = $filter['filters'] ?? [];

        if (empty($filters)) {
            return ['match_all' => new stdClass()];
        }

        $subQueries = [];
        foreach ($filters as $subFilter) {
            if (isset($subFilter['field'])) {
                $field      = $subFilter['field'];
                $operator   = $subFilter['operator'];
                $value      = $subFilter['value'] ?? null;
                $ignoreCase = $subFilter['ignoreCase'] ?? true;
                $fieldInfo  = $this->getFieldInfo($field, $indexMapping);
                $subQuery   = $this->buildElasticQueryForOperator($field, $operator, $value, $ignoreCase, $fieldInfo['type']);

                if ($subFilter['not'] ?? false) {
                    $subQuery = ['bool' => ['must_not' => [$subQuery]]];
                }

                $subQueries[] = $subQuery;
            } else {
                $subQueries[] = $this->buildComplexFilterWithoutNesting($subFilter, $indexMapping);
            }
        }

        if (empty($subQueries)) {
            return ['match_all' => new stdClass()];
        }

        if (count($subQueries) === 1) {
            return $subQueries[0];
        }

        if ($logic === 'or') {
            return ['bool' => ['should' => $subQueries, 'minimum_should_match' => 1]];
        }

        return ['bool' => ['must' => $subQueries]];
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue */
    private function getFilterNestedPath(array $filter, array $indexMapping): string|null
    {
        if (isset($filter['field'])) {
            return $this->getFieldInfo($filter['field'], $indexMapping)['nestedPath'];
        }

        if (isset($filter['filters']) && is_array($filter['filters'])) {
            foreach ($filter['filters'] as $subFilter) {
                $nestedPath = $this->getFilterNestedPath($subFilter, $indexMapping);
                if ($nestedPath !== null) {
                    return $nestedPath;
                }
            }
        }

        return null;
    }

    /** @phpstan-ignore missingType.iterableValue */
    private function buildElasticQueryForOperator(string $field, string $operator, mixed $value, bool $ignoreCase, string|null $fieldType): array
    {
        switch ($operator) {
            case FilterExpression::OP_EQ:
                if ($ignoreCase && $fieldType !== 'keyword') {
                    return ['match' => [$field => ['query' => $value, 'operator' => 'and']]];
                }

                return ['term' => [$field => $value]];

            case FilterExpression::OP_NEQ:
                $termQuery = $ignoreCase && $fieldType !== 'keyword'
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
                    return ['wildcard' => [$field => ['value' => mb_strtolower($value) . '*', 'case_insensitive' => true]]];
                }

                return ['prefix' => [$field => $value]];

            case FilterExpression::OP_DOES_NOT_START_WITH:
                $prefixQuery = $ignoreCase
                    ? ['wildcard' => [$field => ['value' => mb_strtolower($value) . '*', 'case_insensitive' => true]]]
                    : ['prefix' => [$field => $value]];

                return ['bool' => ['must_not' => [$prefixQuery]]];

            case FilterExpression::OP_ENDS_WITH:
                $pattern       = $ignoreCase ? '*' . mb_strtolower($value) : '*' . $value;
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return $wildcardQuery;

            case FilterExpression::OP_DOES_NOT_END_WITH:
                $pattern       = $ignoreCase ? '*' . mb_strtolower($value) : '*' . $value;
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return ['bool' => ['must_not' => [$wildcardQuery]]];

            case FilterExpression::OP_CONTAINS:
                $pattern       = $ignoreCase ? '*' . mb_strtolower($value) . '*' : '*' . $value . '*';
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return $wildcardQuery;

            case FilterExpression::OP_DOES_NOT_CONTAIN:
                $pattern       = $ignoreCase ? '*' . mb_strtolower($value) . '*' : '*' . $value . '*';
                $wildcardQuery = ['wildcard' => [$field => $pattern]];
                if ($ignoreCase) {
                    $wildcardQuery['wildcard'][$field] = ['value' => $pattern, 'case_insensitive' => true];
                }

                return ['bool' => ['must_not' => [$wildcardQuery]]];

            case FilterExpression::OP_IS_EMPTY:
                return [
                    'bool' => [
                        'should' => [
                            ['bool' => ['must_not' => [['exists' => ['field' => $field]]]]],
                            ['term' => [$field => '']],
                        ],
                        'minimum_should_match' => 1,
                    ],
                ];

            case FilterExpression::OP_IS_NOT_EMPTY:
                return [
                    'bool' => [
                        'must' => [['exists' => ['field' => $field]]],
                        'must_not' => [['term' => [$field => '']]],
                    ],
                ];

            case FilterExpression::OP_IN_LIST:
                if (! is_array($value)) {
                    $value = [$value];
                }

                if ($fieldType === 'text') {
                    $shouldQueries = [];
                    foreach ($value as $item) {
                        $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                    }

                    return ['bool' => ['should' => $shouldQueries, 'minimum_should_match' => 1]];
                }

                return ['terms' => [$field => $value]];

            case FilterExpression::OP_NOT_IN_LIST:
                if (! is_array($value)) {
                    $value = [$value];
                }

                if ($fieldType === 'text') {
                    $shouldQueries = [];
                    foreach ($value as $item) {
                        $shouldQueries[] = ['match' => [$field => ['query' => $item, 'operator' => 'and']]];
                    }

                    return ['bool' => ['must_not' => [['bool' => ['should' => $shouldQueries, 'minimum_should_match' => 1]]]]];
                }

                return ['bool' => ['must_not' => [['terms' => [$field => $value]]]]];

            default:
                return ['term' => [$field => $value]];
        }
    }

    /** @phpstan-ignore missingType.iterableValue, missingType.iterableValue */
    private function getFieldInfo(string $field, array $indexMapping): array
    {
        if (isset($indexMapping[$field])) {
            return [
                'type' => $indexMapping[$field]['type'] ?? null,
                'nestedPath' => $indexMapping[$field]['nestedPath'] ?? null,
            ];
        }

        $fieldParts     = explode('.', $field);
        $currentMapping = $indexMapping;

        foreach ($fieldParts as $part) {
            if (! isset($currentMapping[$part])) {
                break;
            }

            $currentMapping = $currentMapping[$part];
            if (isset($currentMapping['type'])) {
                return [
                    'type' => $currentMapping['type'],
                    'nestedPath' => $currentMapping['nestedPath'] ?? null,
                ];
            }

            if (! isset($currentMapping['properties'])) {
                continue;
            }

            $currentMapping = $currentMapping['properties'];
        }

        return ['type' => null, 'nestedPath' => null];
    }
}
