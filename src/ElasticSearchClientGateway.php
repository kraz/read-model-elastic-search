<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use Webmozart\Assert\Assert;

abstract class ElasticSearchClientGateway implements ElasticSearchClientInterface
{
    public function __construct(
        protected readonly ElasticSearchClientInterface $api,
        protected readonly ElasticSearchDenormalizerInterface $denormalizer,
    ) {
    }

    public function search(array $query = [], ?string $index = null): ElasticSearchResponse
    {
        return $this->api->search($query, $index);
    }

    public function getMapping(?string $index = null): ElasticSearchResponse
    {
        return $this->api->getMapping($index);
    }

    public function getFlattenedMapping(?string $index = null, ?callable $mappingExtractor = null): array
    {
        return $this->api->getFlattenedMapping($index, $mappingExtractor);
    }

    /**
     * @phpstan-template T of object
     *
     * @phpstan-param array<string, mixed>|null $query
     * @phpstan-param class-string<T>|null $responseClassName
     *
     * @phpstan-return ($responseClassName is class-string<T> ? T : array<string, mixed>|T[])
     */
    protected function handleRead(?array $query = null, ?string $responseClassName = null, ?string $index = null): object|array
    {
        $query ??= ['query' => ['match_all' => new \stdClass()], 'from' => 0, 'size' => 10];

        $response = $this->api->search($query, $index);
        $result = $response->getResult();

        $size = $query['size'] ?? 0;
        Assert::integer($size);

        $from = $query['from'] ?? 0;
        Assert::integer($from);

        $total = $result['hits']['total'] ?? 0;
        if (\is_array($total)) {
            /** @var int|null $total */
            $total = $total['value'] ?? 0;
        }
        Assert::integer($total);

        /** @var array<int, array<string, mixed>>|null $hits */
        $hits = $result['hits']['hits'] ?? null;
        Assert::isArray($hits);

        $payload = [
            'data' => array_column($hits, '_source'),
            'page' => $size > 0 ? (int) ceil($from / $size) + 1 : 1,
            'total' => $total,
        ];

        if (null !== $responseClassName) {
            /** @var T|T[] $searchResponse */
            $searchResponse = $this->denormalizer->denormalize($payload, $responseClassName);
            if (str_ends_with($responseClassName, '[]')) {
                Assert::isArray($searchResponse);
            } else {
                Assert::isInstanceOf($searchResponse, $responseClassName);
            }
        } else {
            $searchResponse = $payload;
        }

        return $searchResponse;
    }
}
