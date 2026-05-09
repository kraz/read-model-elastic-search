<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\QueryStrategy;

/**
 * Strategy interface for building ElasticSearch version-specific queries.
 */
interface QueryStrategyInterface
{
    /**
     * Builds a full-text search query combined with a filter query.
     *
     * @phpstan-param array<string, mixed>|null $filterQuery
     *
     * @phpstan-return array<string, mixed>
     */
    public function buildFullTextSearchWithFilter(string $term, array|null $filterQuery): array;

    /**
     * Returns the appropriate field name for sorting.
     */
    public function getSortableField(string $field, string|null $fieldType): string;

    /**
     * Returns the unmapped_type value for a given field type.
     */
    public function getUnmappedType(string|null $fieldType): string;

    /**
     * Extracts the mapping properties from the raw mapping API response.
     *
     * @phpstan-param array<string, mixed> $rawMapping
     *
     * @phpstan-return array<string, mixed>
     */
    public function extractMappingProperties(array $rawMapping, string $index): array;

    /**
     * Extracts the total hits count from the search response.
     *
     * @phpstan-param array<string, mixed> $result
     */
    public function extractHitsTotal(array $result): int;
}
