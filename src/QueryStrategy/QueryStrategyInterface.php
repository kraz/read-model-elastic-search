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

    /**
     * Whether this strategy can express cursor (keyset) pagination natively.
     *
     * Returning false instructs the DataSource to refuse cursor mode for this version
     * of Elasticsearch rather than emulate it on top of older query primitives.
     */
    public function supportsCursorPagination(): bool;

    /**
     * Build the per-request params that wire a cursor anchor into the Elasticsearch
     * search body — typically `search_after` plus opt-outs like `track_total_hits`.
     *
     * The returned array is merged into the search params built from the filter and
     * sort. When `$searchAfter` is null this is the first page (no anchor yet) and the
     * strategy should still contribute any cursor-mode opt-outs.
     *
     * @phpstan-param list<mixed>|null $searchAfter
     *
     * @phpstan-return array<string, mixed>
     */
    public function buildCursorParams(array|null $searchAfter): array;
}
