<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\QueryStrategy;

use function is_array;
use function key;

/**
 * ElasticSearch 9.x query strategy.
 *
 * Uses `bool` with `filter` instead of `filtered`, a `catch_all` field (via `copy_to`)
 * instead of `_all`, `.keyword` subfield for sorting text fields, and the modern
 * mapping/response structure.
 */
final class QueryStrategy9x implements QueryStrategyInterface
{
    public function __construct(
        private readonly string $catchAllField = 'catch_all',
    ) {
    }

    public function buildFullTextSearchWithFilter(string $term, array|null $filterQuery): array
    {
        $fullTextQuery = [
            'match' => [
                $this->catchAllField => [
                    'operator' => 'and',
                    'query' => $term,
                ],
            ],
        ];

        if (is_array($filterQuery)) {
            return [
                'bool' => [
                    'must' => [$fullTextQuery],
                    'filter' => [$filterQuery],
                ],
            ];
        }

        return $fullTextQuery;
    }

    public function getSortableField(string $field, string|null $fieldType): string
    {
        if ($fieldType === 'text') {
            return $field . '.keyword';
        }

        return $field;
    }

    public function getUnmappedType(string|null $fieldType): string
    {
        return match ($fieldType) {
            'text' => 'keyword',
            'string' => 'keyword',
            'long', 'integer', 'short', 'byte' => 'long',
            'double', 'float', 'half_float', 'scaled_float' => 'double',
            'date' => 'date',
            'boolean' => 'boolean',
            'keyword' => 'keyword',
            default => 'keyword',
        };
    }

    public function extractMappingProperties(array $rawMapping, string $index): array
    {
        return $rawMapping[key($rawMapping) ?? '']['mappings']['properties'] ?? [];
    }

    public function extractHitsTotal(array $result): int
    {
        $total = $result['hits']['total'] ?? 0;

        if (is_array($total)) {
            return (int) ($total['value'] ?? 0);
        }

        return (int) $total;
    }
}
