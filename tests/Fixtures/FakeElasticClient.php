<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Fixtures;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelElasticSearch\ElasticSearchReadClientInterface;
use Nyholm\Psr7\Factory\Psr17Factory;

use function array_slice;
use function array_values;
use function ceil;
use function count;
use function is_array;
use function is_string;
use function json_encode;
use function max;
use function str_ends_with;
use function strlen;
use function substr;
use function usort;

/**
 * In-memory stub for ElasticSearch client.
 *
 * For non-cursor reads (the `read()` path) it slices the seeded items based on
 * the `from`/`size` params sent by DataSource, so pagination tests work as expected.
 *
 * For cursor reads (which call `search()` directly) it also honors `sort` and
 * `search_after`, and emits a per-hit `sort` array in the response so the cursor
 * path can extract the anchor values without touching `_source`.
 *
 * @implements ElasticSearchReadClientInterface<array<string, mixed>>
 */
final class FakeElasticClient implements ElasticSearchClientInterface, ElasticSearchReadClientInterface
{
    /** @var array<string, mixed>|null */
    private array|null $lastReadParams = null;

    /** @var array<string, mixed>|null */
    private array|null $lastSearchParams = null;

    /**
     * @param list<array<string, mixed>> $items
     * @param array<string, mixed> $flattenedMapping
     */
    public function __construct(
        private readonly array $items = [],
        private readonly array $flattenedMapping = [],
    ) {
    }

    /** @return ReadResponse<array<string, mixed>> */
    public function read(array $query = [], string|null $index = null): ReadResponse
    {
        $this->lastReadParams = $query;

        $from  = (int) ($query['from'] ?? 0);
        $size  = isset($query['size']) ? (int) $query['size'] : null;
        $total = count($this->items);
        $slice = $size !== null ? array_slice($this->items, $from, $size) : array_slice($this->items, $from);
        $page  = ($size !== null && $size > 0) ? (int) ceil($from / $size) + 1 : 1;

        return ReadResponse::create($slice, max(1, $page), $total);
    }

    public function search(array $query = [], string|null $index = null): ElasticSearchResponse
    {
        $this->lastSearchParams = $query;

        $items    = $this->items;
        $sortSpec = $query['sort'] ?? [];
        /** @var list<array{0: string, 1: string}> $compiled */
        $compiled = is_array($sortSpec) ? $this->compileSort($sortSpec) : [];

        if (count($compiled) > 0) {
            usort($items, function (array $a, array $b) use ($compiled): int {
                foreach ($compiled as [$field, $direction]) {
                    $cmp = $this->compareValues($a[$field] ?? null, $b[$field] ?? null);
                    if ($cmp !== 0) {
                        return $direction === 'desc' ? -$cmp : $cmp;
                    }
                }

                return 0;
            });
        }

        $searchAfter = $query['search_after'] ?? null;
        if (is_array($searchAfter) && count($compiled) > 0) {
            /** @var list<mixed> $searchAfter */
            $searchAfter = array_values($searchAfter);
            $startIndex  = null;
            foreach ($items as $i => $item) {
                if (! $this->rowComesAfter($item, $compiled, $searchAfter)) {
                    continue;
                }

                $startIndex = $i;
                break;
            }

            $items = $startIndex !== null ? array_slice($items, $startIndex) : [];
        }

        $size  = isset($query['size']) ? (int) $query['size'] : count($items);
        $items = array_slice($items, 0, $size);

        $hits = [];
        foreach ($items as $item) {
            $hit = ['_source' => $item];
            if (count($compiled) > 0) {
                $sortVals = [];
                foreach ($compiled as [$field]) {
                    /** @var mixed $value */
                    $value      = $item[$field] ?? null;
                    $sortVals[] = $value;
                }

                $hit['sort'] = $sortVals;
            }

            $hits[] = $hit;
        }

        $payload = [
            'hits' => [
                'total' => ['value' => count($this->items)],
                'hits'  => $hits,
            ],
        ];

        $factory = new Psr17Factory();
        $body    = $factory->createStream((string) json_encode($payload));

        return new ElasticSearchResponse($factory->createResponse(200)->withBody($body));
    }

    public function getMapping(string|null $index = null): ElasticSearchResponse
    {
        $factory = new Psr17Factory();
        $body    = $factory->createStream('{}');

        return new ElasticSearchResponse($factory->createResponse(200)->withBody($body));
    }

    /** @return array<string, mixed> */
    public function getFlattenedMapping(string|null $index = null, callable|null $mappingExtractor = null): array
    {
        return $this->flattenedMapping;
    }

    /** @return array<string, mixed>|null */
    public function getLastReadParams(): array|null
    {
        return $this->lastReadParams;
    }

    /** @return array<string, mixed>|null */
    public function getLastSearchParams(): array|null
    {
        return $this->lastSearchParams;
    }

    /**
     * @param array<int|string, mixed> $sortSpec
     *
     * @return list<array{0: string, 1: string}>
     */
    private function compileSort(array $sortSpec): array
    {
        $compiled = [];
        foreach ($sortSpec as $item) {
            if (! is_array($item)) {
                continue;
            }

            /** @var mixed $opts */
            foreach ($item as $field => $opts) {
                if (! is_string($field)) {
                    continue;
                }

                $direction = 'asc';
                if (is_array($opts)) {
                    /** @var mixed $order */
                    $order     = $opts['order'] ?? 'asc';
                    $direction = is_string($order) ? $order : 'asc';
                } elseif (is_string($opts)) {
                    $direction = $opts;
                }

                $baseField = $field;
                if (str_ends_with($baseField, '.keyword')) {
                    $baseField = substr($baseField, 0, -strlen('.keyword'));
                }

                $compiled[] = [$baseField, $direction];
            }
        }

        return $compiled;
    }

    private function compareValues(mixed $a, mixed $b): int
    {
        if ($a === null && $b === null) {
            return 0;
        }

        if ($a === null) {
            return -1;
        }

        if ($b === null) {
            return 1;
        }

        return $a <=> $b;
    }

    /**
     * Standard keyset semantics: the row is "after" the anchor when, scanning the
     * sort fields in order, the first non-equal value is on the correct side of the
     * direction's comparator.
     *
     * @param array<string, mixed>             $row
     * @param list<array{0: string, 1: string}> $compiled
     * @param list<mixed>                       $searchAfter
     */
    private function rowComesAfter(array $row, array $compiled, array $searchAfter): bool
    {
        foreach ($compiled as $i => [$field, $direction]) {
            $cmp = $this->compareValues($row[$field] ?? null, $searchAfter[$i] ?? null);
            if ($cmp === 0) {
                continue;
            }

            return $direction === 'desc' ? $cmp < 0 : $cmp > 0;
        }

        return false;
    }
}
