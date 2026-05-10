<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Fixtures;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelElasticSearch\ElasticSearchReadClientInterface;
use Nyholm\Psr7\Factory\Psr17Factory;

use function array_slice;
use function ceil;
use function count;
use function json_encode;
use function max;

/**
 * In-memory stub for ElasticSearch client. Slices the seeded items based on
 * the `from`/`size` params sent by DataSource, so pagination tests work as expected.
 *
 * @implements ElasticSearchReadClientInterface<array<string, mixed>>
 */
final class FakeElasticClient implements ElasticSearchClientInterface, ElasticSearchReadClientInterface
{
    /** @var array<string, mixed>|null */
    private array|null $lastReadParams = null;

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
        $factory = new Psr17Factory();
        $body    = $factory->createStream((string) json_encode(['hits' => ['total' => ['value' => 0], 'hits' => []]]));

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
}
