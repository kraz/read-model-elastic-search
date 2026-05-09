<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use stdClass;
use Webmozart\Assert\Assert;

use function array_column;
use function ceil;
use function is_array;

abstract class ElasticSearchClientGateway implements ElasticSearchClientInterface
{
    public function __construct(
        protected readonly ElasticSearchClientInterface $api,
        protected readonly ElasticSearchDenormalizerInterface $denormalizer,
    ) {
    }

    public function search(array $query = [], string|null $index = null): ElasticSearchResponse
    {
        return $this->api->search($query, $index);
    }

    public function getMapping(string|null $index = null): ElasticSearchResponse
    {
        return $this->api->getMapping($index);
    }

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
        Assert::integer($size);

        $from = $query['from'] ?? 0;
        Assert::integer($from);

        $total = $result['hits']['total'] ?? 0;
        if (is_array($total)) {
            /** @phpstan-var int|null $total */
            $total = $total['value'] ?? 0;
        }

        Assert::integer($total);

        /** @phpstan-var array<int, array<string, mixed>>|null $hits */
        $hits = $result['hits']['hits'] ?? null;
        Assert::isArray($hits);

        $payload = [
            'data' => array_column($hits, '_source'),
            'page' => $size > 0 ? (int) ceil($from / $size) + 1 : 1,
            'total' => $total,
        ];

        if ($responseClassName !== null) {
            $searchResponse = $this->denormalizer->denormalize($payload, $responseClassName);
            Assert::isInstanceOf($searchResponse, $responseClassName);
        } else {
            $searchResponse = $payload;
        }

        return $searchResponse;
    }
}
