<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\QueryStrategy;

/**
 * ElasticSearch 1.x query strategy.
 *
 * Uses the `filtered` query wrapper, the `_all` field for full-text search,
 * and the legacy mapping/response structure.
 */
final class QueryStrategy1x implements QueryStrategyInterface
{
    public function buildFullTextSearchWithFilter(string $term, ?array $filterQuery): array
    {
        if (\is_array($filterQuery)) {
            return [
                'filtered' => [
                    'query' => [
                        'match' => [
                            '_all' => [
                                'operator' => 'and',
                                'query' => $term,
                            ],
                        ],
                    ],
                    'filter' => $filterQuery,
                ],
            ];
        }

        return [
            'match' => [
                '_all' => [
                    'operator' => 'and',
                    'query' => $term,
                ],
            ],
        ];
    }

    public function getSortableField(string $field, ?string $fieldType): string
    {
        return $field;
    }

    public function getUnmappedType(?string $fieldType): string
    {
        return match ($fieldType) {
            'text' => 'string',
            'string' => 'string',
            'long', 'integer', 'short', 'byte' => 'long',
            'double', 'float', 'half_float', 'scaled_float' => 'double',
            'date' => 'date',
            'boolean' => 'boolean',
            'keyword' => 'string',
            default => 'string',
        };
    }

    public function extractMappingProperties(array $rawMapping, string $index): array
    {
        return $rawMapping[key($rawMapping) ?? '']['mappings'][$index]['properties'] ?? [];
    }

    public function extractHitsTotal(array $result): int
    {
        $total = $result['hits']['total'] ?? 0;

        return (int) $total;
    }
}
