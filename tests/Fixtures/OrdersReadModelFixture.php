<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Fixtures;

use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModelElasticSearch\DataSource;
use Kraz\ReadModelElasticSearch\DataSourceBuilder;
use Kraz\ReadModelElasticSearch\ElasticRawQuerySearchReadModelInterface;
use Kraz\ReadModelElasticSearch\ElasticSearchReadClientInterface;
use Kraz\ReadModelElasticSearch\ElasticSearchReadDataProvider;
use Kraz\ReadModelElasticSearch\FullTextSearchReadModelInterface;

/**
 * Mirrors the production read model pattern: uses ElasticSearchReadDataProvider,
 * defines FIELD_* constants, and wires in the API through createDataSource().
 *
 * @implements ReadDataProviderInterface<array<string, mixed>>
 * @implements FullTextSearchReadModelInterface<array<string, mixed>>
 * @implements ElasticRawQuerySearchReadModelInterface<array<string, mixed>>
 */
final class OrdersReadModelFixture implements ReadDataProviderInterface, FullTextSearchReadModelInterface, ElasticRawQuerySearchReadModelInterface
{
    /** @use ElasticSearchReadDataProvider<array<string, mixed>> */
    use ElasticSearchReadDataProvider;

    public const string FIELD_ORDER_ID    = 'orderId';
    public const string FIELD_STATUS      = 'status';
    public const string FIELD_CLIENT_NAME = 'clientName';
    public const string FIELD_ORDER_VALUE = 'orderValue';

    /** @param ElasticSearchClientInterface&ElasticSearchReadClientInterface<array<string, mixed>> $api */
    public function __construct(
        private readonly ElasticSearchClientInterface&ElasticSearchReadClientInterface $api,
    ) {
    }

    /** @return DataSource<array<string, mixed>> */
    protected function createDataSource(): DataSource
    {
        return new DataSourceBuilder()
            ->withRootIdentifier(self::FIELD_ORDER_ID)
            ->create($this->api);
    }
}
