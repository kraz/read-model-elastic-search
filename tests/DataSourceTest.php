<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests;

use Kraz\ReadModel\Pagination\InMemoryPaginator;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelElasticSearch\DataSource;
use Kraz\ReadModelElasticSearch\DataSourceBuilder;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\FakeElasticClient;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\OrdersReadModelFixture;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\Specifications\OrderValueAboveSpecification;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\Specifications\StatusEqualsSpecification;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_column;
use function is_array;
use function iterator_to_array;

#[CoversClass(DataSource::class)]
final class DataSourceTest extends TestCase
{
    /** @return list<array<string, mixed>> */
    private function seed(): array
    {
        return [
            ['orderId' => 'A001', 'status' => 'new',     'clientName' => 'Alice',   'orderValue' => 100.0],
            ['orderId' => 'A002', 'status' => 'new',     'clientName' => 'Bob',     'orderValue' => 200.0],
            ['orderId' => 'A003', 'status' => 'shipped', 'clientName' => 'Charlie', 'orderValue' => 150.0],
            ['orderId' => 'A004', 'status' => 'shipped', 'clientName' => 'Dave',    'orderValue' => 300.0],
            ['orderId' => 'A005', 'status' => 'new',     'clientName' => 'Eve',     'orderValue' => 50.0],
        ];
    }

    /** @return DataSource<array<string, mixed>> */
    private function makeDs(FakeElasticClient|null $client = null): DataSource
    {
        $client ??= new FakeElasticClient($this->seed());

        /** @var DataSource<array<string, mixed>> $ds */
        $ds = new DataSource($client);

        return $ds;
    }

    /**
     * @param iterable<mixed> $items
     *
     * @return list<string>
     */
    private function orderIds(iterable $items): array
    {
        $ids = [];
        foreach ($items as $item) {
            if (is_array($item) && isset($item['orderId'])) {
                $ids[] = (string) $item['orderId'];
            }
        }

        return $ids;
    }

    // -------------------------------------------------------------------------
    // Data retrieval
    // -------------------------------------------------------------------------

    public function testDataReturnsAllSeededItems(): void
    {
        self::assertCount(5, $this->makeDs()->data());
    }

    public function testGetIteratorYieldsAllItems(): void
    {
        $items = iterator_to_array($this->makeDs()->getIterator());

        self::assertCount(5, $items);
    }

    public function testDataItemsContainExpectedFields(): void
    {
        $data = $this->makeDs()->data();

        self::assertSame('A001', $data[0]['orderId']);
        self::assertSame('Alice', $data[0]['clientName']);
    }

    // -------------------------------------------------------------------------
    // count / totalCount / isEmpty
    // -------------------------------------------------------------------------

    public function testCountWithoutPaginationReturnsTotalItems(): void
    {
        self::assertSame(5, $this->makeDs()->count());
    }

    public function testTotalCountReturnsTotalFromPayload(): void
    {
        self::assertSame(5, $this->makeDs()->totalCount());
    }

    public function testIsEmptyReturnsFalseWhenDataExists(): void
    {
        self::assertFalse($this->makeDs()->isEmpty());
    }

    public function testIsEmptyReturnsTrueForEmptyClient(): void
    {
        $ds = new DataSource(new FakeElasticClient([]));

        self::assertTrue($ds->isEmpty());
    }

    // -------------------------------------------------------------------------
    // Pagination
    // -------------------------------------------------------------------------

    public function testWithPaginationEnablesPagination(): void
    {
        self::assertTrue($this->makeDs()->withPagination(1, 2)->isPaginated());
    }

    public function testWithoutPaginationDisablesPagination(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2)->withoutPagination();

        self::assertFalse($ds->isPaginated());
        self::assertNull($ds->paginator());
    }

    public function testPaginatorReturnsNullWithoutPagination(): void
    {
        self::assertNull($this->makeDs()->paginator());
    }

    public function testPaginatorReturnsInMemoryPaginatorWhenPaginated(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2);

        self::assertInstanceOf(InMemoryPaginator::class, $ds->paginator());
    }

    public function testPaginatorIsCachedPerInstance(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2);

        self::assertSame($ds->paginator(), $ds->paginator());
    }

    public function testFirstPageReturnsFirstItems(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2);

        self::assertSame(['A001', 'A002'], $this->orderIds($ds->data()));
    }

    public function testSecondPageReturnsNextItems(): void
    {
        $ds = $this->makeDs()->withPagination(2, 2);

        self::assertSame(['A003', 'A004'], $this->orderIds($ds->data()));
    }

    public function testLastPartialPageReturnsRemainder(): void
    {
        $ds = $this->makeDs()->withPagination(3, 2);

        self::assertSame(['A005'], $this->orderIds($ds->data()));
    }

    public function testCountWhenPaginatedReturnsItemsOnCurrentPage(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2);

        self::assertSame(2, $ds->count());
    }

    public function testTotalCountIgnoresPagination(): void
    {
        $ds = $this->makeDs()->withPagination(1, 2);

        self::assertSame(5, $ds->totalCount());
    }

    public function testPaginationParamsAreSentToClient(): void
    {
        $client = new FakeElasticClient($this->seed());
        $ds     = new DataSource($client);

        $ds->withPagination(2, 3)->data();

        $params = $client->getLastReadParams();
        self::assertNotNull($params);
        self::assertSame(3, $params['from']);
        self::assertSame(3, $params['size']);
    }

    // -------------------------------------------------------------------------
    // Limit
    // -------------------------------------------------------------------------

    public function testWithLimitRestrictsItemCount(): void
    {
        $ds = $this->makeDs()->withLimit(3);

        self::assertSame(['A001', 'A002', 'A003'], $this->orderIds($ds->data()));
    }

    public function testWithLimitAndOffsetSkipsItems(): void
    {
        $ds = $this->makeDs()->withLimit(2, 2);

        self::assertSame(['A003', 'A004'], $this->orderIds($ds->data()));
    }

    public function testWithoutLimitRestoresAllItems(): void
    {
        $ds = $this->makeDs()->withLimit(2)->withoutLimit();

        self::assertCount(5, $ds->data());
    }

    public function testLimitParamsAreSentToClient(): void
    {
        $client = new FakeElasticClient($this->seed());
        $ds     = new DataSource($client);

        $ds->withLimit(3)->data();

        $params = $client->getLastReadParams();
        self::assertNotNull($params);
        self::assertSame(3, $params['size']);
        self::assertArrayNotHasKey('from', $params);
    }

    // -------------------------------------------------------------------------
    // getResult
    // -------------------------------------------------------------------------

    public function testGetResultReturnsReadResponseWhenNotValue(): void
    {
        $result = $this->makeDs()->getResult();

        self::assertInstanceOf(ReadResponse::class, $result);
        self::assertSame(1, $result->page);
        self::assertSame(5, $result->total);
    }

    public function testGetResultOnPaginatedSourceContainsCorrectPage(): void
    {
        $result = $this->makeDs()->withPagination(2, 2)->getResult();

        self::assertInstanceOf(ReadResponse::class, $result);
        self::assertSame(2, $result->page);
        self::assertSame(5, $result->total);
        self::assertNotNull($result->data);
        self::assertCount(2, $result->data);
    }

    public function testGetResultReturnsArrayForValueQuery(): void
    {
        $client = new FakeElasticClient([
            ['orderId' => 'A001', 'status' => 'new', 'clientName' => 'Alice', 'orderValue' => 100.0],
            ['orderId' => 'A003', 'status' => 'shipped', 'clientName' => 'Charlie', 'orderValue' => 150.0],
        ]);

        /** @var DataSource<array<string, mixed>> $ds */
        $ds = (new DataSourceBuilder())->withRootIdentifier('orderId')->create($client);
        $ds = $ds->withQueryExpression(QueryExpression::create()->withValues(['A001', 'A003']));

        $result = $ds->getResult();

        self::assertIsArray($result);
    }

    // -------------------------------------------------------------------------
    // QueryExpression
    // -------------------------------------------------------------------------

    public function testQueryExpressionsInitiallyEmpty(): void
    {
        self::assertSame([], $this->makeDs()->queryExpressions());
    }

    public function testWithQueryExpressionAddsToStack(): void
    {
        $ds = $this->makeDs();
        $qe = QueryExpression::create();
        $ds = $ds->withQueryExpression($qe);

        self::assertSame([$qe], $ds->queryExpressions());
    }

    public function testWithoutQueryExpressionClearsStack(): void
    {
        $ds = $this->makeDs()->withQueryExpression(QueryExpression::create());

        self::assertSame([], $ds->withoutQueryExpression()->queryExpressions());
    }

    public function testWithoutQueryExpressionUndoRestoresPrevious(): void
    {
        $ds  = $this->makeDs();
        $qe1 = QueryExpression::create();
        $qe2 = QueryExpression::create();

        $stacked = $ds->withQueryExpression($qe1)->withQueryExpression($qe2, true);
        $back    = $stacked->withoutQueryExpression(true);

        self::assertSame([$qe1], $back->queryExpressions());
    }

    public function testValuesQuerySortsDataByGivenOrder(): void
    {
        // Seed only the three items in the values list; the real ES would filter to
        // just those IDs, so the fake client is pre-filtered the same way.
        $client = new FakeElasticClient([
            ['orderId' => 'A001', 'status' => 'new',     'clientName' => 'Alice',   'orderValue' => 100.0],
            ['orderId' => 'A002', 'status' => 'new',     'clientName' => 'Bob',     'orderValue' => 200.0],
            ['orderId' => 'A003', 'status' => 'shipped', 'clientName' => 'Charlie', 'orderValue' => 150.0],
        ]);

        /** @var DataSource<array<string, mixed>> $ds */
        $ds = (new DataSourceBuilder())->withRootIdentifier('orderId')->create($client);
        $qe = QueryExpression::create()->withValues(['A003', 'A001', 'A002']);
        $ds = $ds->withQueryExpression($qe);

        self::assertSame(['A003', 'A001', 'A002'], $this->orderIds($ds->data()));
    }

    // -------------------------------------------------------------------------
    // withQueryModifier
    // -------------------------------------------------------------------------

    public function testWithQueryModifierThrowsLogicException(): void
    {
        $this->expectException(LogicException::class);

        $this->makeDs()->withQueryModifier(static function (): void {
        });
    }

    // -------------------------------------------------------------------------
    // Full-text search
    // -------------------------------------------------------------------------

    public function testWithFullTextSearchReturnsNewInstance(): void
    {
        $ds    = $this->makeDs();
        $clone = $ds->withFullTextSearch('hello');

        self::assertNotSame($ds, $clone);
    }

    public function testWithoutFullTextSearchReturnsNewInstance(): void
    {
        $ds    = $this->makeDs()->withFullTextSearch('hello');
        $clone = $ds->withoutFullTextSearch();

        self::assertNotSame($ds, $clone);
    }

    public function testFullTextSearchTermIsIncludedInQuery(): void
    {
        $client = new FakeElasticClient($this->seed());
        $ds     = (new DataSource($client))->withFullTextSearch('alice');

        $ds->data();

        $params = $client->getLastReadParams();
        self::assertNotNull($params);
        self::assertArrayHasKey('query', $params);
    }

    // -------------------------------------------------------------------------
    // Raw query search
    // -------------------------------------------------------------------------

    public function testWithRawQuerySearchReturnsNewInstance(): void
    {
        $ds    = $this->makeDs();
        $clone = $ds->withRawQuerySearch('{"query":{"match_all":{}}}');

        self::assertNotSame($ds, $clone);
    }

    public function testWithoutRawQuerySearchReturnsNewInstance(): void
    {
        $ds    = $this->makeDs()->withRawQuerySearch('{}');
        $clone = $ds->withoutRawQuerySearch();

        self::assertNotSame($ds, $clone);
    }

    public function testRawQuerySearchPassesPayloadDirectlyToClient(): void
    {
        $client   = new FakeElasticClient($this->seed());
        $rawQuery = '{"query":{"match_all":{}},"size":2}';

        (new DataSource($client))->withRawQuerySearch($rawQuery)->data();

        $params = $client->getLastReadParams();
        self::assertNotNull($params);
        self::assertSame(2, $params['size'] ?? null);
    }

    // -------------------------------------------------------------------------
    // Specifications
    // -------------------------------------------------------------------------

    public function testWithSpecificationWithoutLimitThrows(): void
    {
        $this->expectException(LogicException::class);
        $this->makeDs()->withSpecification(new StatusEqualsSpecification('new'))->data();
    }

    public function testWithSpecificationFiltersItemsInPhp(): void
    {
        $ds = $this->makeDs()->withLimit(100)->withSpecification(new StatusEqualsSpecification('new'));

        self::assertSame(['A001', 'A002', 'A005'], $this->orderIds($ds->data()));
    }

    public function testWithSpecificationFiltersByValueRange(): void
    {
        $ds = $this->makeDs()->withLimit(100)->withSpecification(new OrderValueAboveSpecification(150.0));

        self::assertSame(['A002', 'A004'], $this->orderIds($ds->data()));
    }

    public function testMultipleSpecificationsAreCombinedWithAnd(): void
    {
        $ds = $this->makeDs()
            ->withLimit(100)
            ->withSpecification(new StatusEqualsSpecification('new'))
            ->withSpecification(new OrderValueAboveSpecification(100.0), true);

        self::assertSame(['A002'], $this->orderIds($ds->data()));
    }

    public function testInvertedSpecificationFiltersOppositeItems(): void
    {
        $spec = (new StatusEqualsSpecification('new'))->invert();
        $ds   = $this->makeDs()->withLimit(100)->withSpecification($spec);

        self::assertSame(['A003', 'A004'], $this->orderIds($ds->data()));
    }

    public function testWithoutSpecificationClearsAllSpecs(): void
    {
        $ds = $this->makeDs()
            ->withSpecification(new StatusEqualsSpecification('new'))
            ->withoutSpecification();

        self::assertCount(5, $ds->data());
    }

    public function testWithoutSpecificationUndoRestoresPrevious(): void
    {
        $ds = $this->makeDs()
            ->withLimit(100)
            ->withSpecification(new StatusEqualsSpecification('new'))
            ->withSpecification(new OrderValueAboveSpecification(100.0), true);

        $undone = $ds->withoutSpecification(true);

        self::assertSame(['A001', 'A002', 'A005'], $this->orderIds($undone->data()));
    }

    // -------------------------------------------------------------------------
    // Immutability
    // -------------------------------------------------------------------------

    public function testWithMethodsReturnNewInstances(): void
    {
        $ds = $this->makeDs();
        $qe = QueryExpression::create();

        self::assertNotSame($ds, $ds->withPagination(1, 2));
        self::assertNotSame($ds, $ds->withoutPagination());
        self::assertNotSame($ds, $ds->withQueryExpression($qe));
        self::assertNotSame($ds, $ds->withoutQueryExpression());
        self::assertNotSame($ds, $ds->withoutQueryExpression(true));
        self::assertNotSame($ds, $ds->withSpecification(new StatusEqualsSpecification('new')));
        self::assertNotSame($ds, $ds->withoutSpecification());
        self::assertNotSame($ds, $ds->withoutSpecification(true));
        self::assertNotSame($ds, $ds->withFullTextSearch('term'));
        self::assertNotSame($ds, $ds->withoutFullTextSearch());
        self::assertNotSame($ds, $ds->withRawQuerySearch('{}'));
        self::assertNotSame($ds, $ds->withoutRawQuerySearch());
        self::assertNotSame($ds, $ds->withLimit(1));
        self::assertNotSame($ds, $ds->withoutLimit());
        self::assertNotSame($ds, $ds->withoutLimit(true));
        self::assertNotSame($ds, $ds->withItemNormalizer(static fn (mixed $item): mixed => $item));
        self::assertNotSame($ds, $ds->withoutItemNormalizer());
    }

    public function testOriginalIsNotMutatedByWithPagination(): void
    {
        $ds = $this->makeDs();
        $ds->withPagination(1, 2);

        self::assertFalse($ds->isPaginated());
    }

    public function testOriginalIsNotMutatedByWithQueryExpression(): void
    {
        $ds = $this->makeDs();
        $ds->withQueryExpression(QueryExpression::create());

        self::assertSame([], $ds->queryExpressions());
    }

    public function testCloneResetsPayloadCache(): void
    {
        $ds = $this->makeDs();

        $ds->data();

        $client = new FakeElasticClient($this->seed());
        $clone  = (new DataSource($client))->withPagination(1, 2);

        $clone->data();

        self::assertNotNull($client->getLastReadParams());
    }

    // -------------------------------------------------------------------------
    // Item normalizer
    // -------------------------------------------------------------------------

    public function testItemNormalizerIsApplied(): void
    {
        $ds = $this->makeDs()->withItemNormalizer(
            static fn (array $item): string => (string) $item['orderId'],
        );

        $result = $ds->data();

        self::assertSame(['A001', 'A002', 'A003', 'A004', 'A005'], $result);
    }

    public function testWithoutItemNormalizerRemovesNormalizer(): void
    {
        $ds = $this->makeDs()
            ->withItemNormalizer(static fn (array $item): string => (string) $item['orderId'])
            ->withoutItemNormalizer();

        $result = $ds->data();

        self::assertIsArray($result[0]);
    }

    // -------------------------------------------------------------------------
    // specificationsIterator
    // -------------------------------------------------------------------------

    public function testSpecificationsIteratorReturnsMatchingItems(): void
    {
        $result = $this->makeDs()->specificationsIterator([new StatusEqualsSpecification('new')], limit: 2);

        self::assertSame(['A001', 'A002'], $this->orderIds($result));
    }

    public function testSpecificationsIteratorRespectsLimit(): void
    {
        $result = $this->makeDs()->specificationsIterator([new OrderValueAboveSpecification(99.0)], limit: 2);

        self::assertSame(['A001', 'A002'], $this->orderIds($result));
    }

    public function testSpecificationsIteratorRespectsOffset(): void
    {
        $result = $this->makeDs()->specificationsIterator([new StatusEqualsSpecification('new')], limit: 2, offset: 1);

        self::assertSame(['A002', 'A005'], $this->orderIds($result));
    }

    public function testSpecificationsIteratorReturnsEmptyWhenNothingMatches(): void
    {
        $result = $this->makeDs()->specificationsIterator([new StatusEqualsSpecification('cancelled')], limit: 5);

        self::assertSame([], $this->orderIds($result));
    }

    // -------------------------------------------------------------------------
    // Read model fixture integration
    // -------------------------------------------------------------------------

    public function testOrdersReadModelFixtureReturnsAllOrders(): void
    {
        $client = new FakeElasticClient($this->seed());
        $model  = new OrdersReadModelFixture($client);

        self::assertCount(5, $model->data());
    }

    public function testOrdersReadModelFixtureWithPaginationReturnsFistPage(): void
    {
        $client = new FakeElasticClient($this->seed());
        $model  = (new OrdersReadModelFixture($client))->withPagination(1, 2);

        self::assertSame(['A001', 'A002'], array_column($model->data(), 'orderId'));
    }

    public function testOrdersReadModelFixtureWithFullTextSearchReturnsDataSource(): void
    {
        $client = new FakeElasticClient($this->seed());
        $model  = (new OrdersReadModelFixture($client))->withFullTextSearch('alice');

        self::assertCount(5, $model->data());
    }

    public function testOrdersReadModelFixtureWithRawQuerySearchPassesPayload(): void
    {
        $client   = new FakeElasticClient($this->seed());
        $rawQuery = '{"query":{"match_all":{}},"size":1}';
        $model    = (new OrdersReadModelFixture($client))->withRawQuerySearch($rawQuery);

        $result = $model->data();

        self::assertCount(1, $result);
    }

    public function testOrdersReadModelFixtureImmutable(): void
    {
        $client = new FakeElasticClient($this->seed());
        $model  = new OrdersReadModelFixture($client);
        $paged  = $model->withPagination(1, 2);

        self::assertFalse($model->isPaginated());
        self::assertTrue($paged->isPaginated());
    }

    public function testOrdersReadModelFixtureWithQueryExpression(): void
    {
        // Seed only the items in the values list; the real ES would filter to those IDs.
        $client = new FakeElasticClient([
            ['orderId' => 'A001', 'status' => 'new',     'clientName' => 'Alice',   'orderValue' => 100.0],
            ['orderId' => 'A003', 'status' => 'shipped', 'clientName' => 'Charlie', 'orderValue' => 150.0],
        ]);
        $model = new OrdersReadModelFixture($client);
        $qe    = QueryExpression::create()->withValues(['A003', 'A001']);
        $model = $model->withQueryExpression($qe);

        self::assertSame(['A003', 'A001'], array_column($model->data(), 'orderId'));
    }
}
