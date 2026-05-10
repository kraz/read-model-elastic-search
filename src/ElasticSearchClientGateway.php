<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use InvalidArgumentException;
use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use Override;
use stdClass;

use function array_column;
use function ceil;
use function is_array;
use function is_int;

abstract class ElasticSearchClientGateway implements ElasticSearchClientInterface
{
    public function __construct(
        protected readonly ElasticSearchClientInterface $api,
        protected readonly ElasticSearchDenormalizerInterface $denormalizer,
    ) {
    }

    #[Override]
    public function search(array $query = [], string|null $index = null): ElasticSearchResponse
    {
        return $this->api->search($query, $index);
    }

    #[Override]
    public function getMapping(string|null $index = null): ElasticSearchResponse
    {
        return $this->api->getMapping($index);
    }

    #[Override]
    public function getFlattenedMapping(string|null $index = null, callable|null $mappingExtractor = null): array
    {
        return $this->api->getFlattenedMapping($index, $mappingExtractor);
    }

    /**
     * @phpstan-param array<string, mixed>|null $query
     * @phpstan-param class-string<T>|null $responseClassName
     *
     * @phpstan-return ($responseClassName is class-string<T> ? T : array<string, mixed>)
     *
     * @phpstan-template T of object
     */
    protected function handleRead(array|null $query = null, string|null $responseClassName = null, string|null $index = null): object|array
    {
        $query ??= ['query' => ['match_all' => new stdClass()], 'from' => 0, 'size' => 10];

        $response = $this->api->search($query, $index);
        $result   = $response->getResult();

        $size = $query['size'] ?? 0;
        if (! is_int($size)) {
            throw new InvalidArgumentException('Expected query[size] to be an integer.');
        }

        $from = $query['from'] ?? 0;
        if (! is_int($from)) {
            throw new InvalidArgumentException('Expected query[from] to be an integer.');
        }

        $total = $result['hits']['total'] ?? 0;
        if (is_array($total)) {
            /** @phpstan-var int|null $total */
            $total = $total['value'] ?? 0;
        }

        if (! is_int($total)) {
            throw new InvalidArgumentException('Expected hits.total to be an integer.');
        }

        /** @phpstan-var array<int, array<string, mixed>>|null $hits */
        $hits = $result['hits']['hits'] ?? null;
        if (! is_array($hits)) {
            throw new InvalidArgumentException('Expected hits.hits to be an array.');
        }

        $payload = [
            'data' => array_column($hits, '_source'),
            'page' => $size > 0 ? (int) ceil($from / $size) + 1 : 1,
            'total' => $total,
        ];

        if ($responseClassName !== null) {
            $searchResponse = $this->denormalizer->denormalize($payload, $responseClassName);
            if (! ($searchResponse instanceof $responseClassName)) {
                throw new InvalidArgumentException('Expected denormalized response to be an instance of ' . $responseClassName . '.');
            }
        } else {
            $searchResponse = $payload;
        }

        return $searchResponse;
    }
}
