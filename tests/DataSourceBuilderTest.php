<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests;

use InvalidArgumentException;
use Kraz\ReadModelElasticSearch\DataSource;
use Kraz\ReadModelElasticSearch\DataSourceBuilder;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy1x;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\FakeElasticClient;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;

#[CoversClass(DataSourceBuilder::class)]
final class DataSourceBuilderTest extends TestCase
{
    private function makeBuilder(): DataSourceBuilder
    {
        return new DataSourceBuilder();
    }

    private function makeClient(): FakeElasticClient
    {
        return new FakeElasticClient();
    }

    // -------------------------------------------------------------------------
    // Immutability
    // -------------------------------------------------------------------------

    public function testWithDataReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder();
        $clone   = $builder->withData($this->makeClient());

        self::assertNotSame($builder, $clone);
    }

    public function testWithFullTextSearchReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder();
        $clone   = $builder->withFullTextSearch('hello');

        self::assertNotSame($builder, $clone);
    }

    public function testWithoutFullTextSearchReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder()->withFullTextSearch('hello');
        $clone   = $builder->withoutFullTextSearch();

        self::assertNotSame($builder, $clone);
    }

    public function testWithRawQuerySearchReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder();
        $clone   = $builder->withRawQuerySearch('{"query":{}}');

        self::assertNotSame($builder, $clone);
    }

    public function testWithoutRawQuerySearchReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder()->withRawQuerySearch('{}');
        $clone   = $builder->withoutRawQuerySearch();

        self::assertNotSame($builder, $clone);
    }

    public function testWithQueryStrategyReturnsNewInstance(): void
    {
        $builder = $this->makeBuilder();
        $clone   = $builder->withQueryStrategy(new QueryStrategy1x());

        self::assertNotSame($builder, $clone);
    }

    public function testOriginalBuilderIsNotModifiedByWithData(): void
    {
        $builder = $this->makeBuilder();
        $builder->withData($this->makeClient());

        $this->expectException(InvalidArgumentException::class);
        $builder->create();
    }

    // -------------------------------------------------------------------------
    // create() validation
    // -------------------------------------------------------------------------

    public function testCreateThrowsWhenNoDataIsAssigned(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('no data assigned');

        $this->makeBuilder()->create();
    }

public function testCreateWithMockClientReturnsDataSource(): void
    {
        $client     = $this->makeClient();
        $dataSource = $this->makeBuilder()->withData($client)->create();

        self::assertInstanceOf(DataSource::class, $dataSource);
    }

    public function testCreatePassesDataDirectlyToDataSource(): void
    {
        $client     = $this->makeClient();
        $dataSource = $this->makeBuilder()->create($client);

        self::assertInstanceOf(DataSource::class, $dataSource);
    }

    public function testCreateWithQueryStrategyUsesProvidedStrategy(): void
    {
        $client     = $this->makeClient();
        $dataSource = $this->makeBuilder()
            ->withQueryStrategy(new QueryStrategy1x())
            ->create($client);

        self::assertInstanceOf(DataSource::class, $dataSource);
    }

    // -------------------------------------------------------------------------
    // Full-text and raw-query search forwarded to DataSource
    // -------------------------------------------------------------------------

    public function testCreateWithFullTextSearchBuildsDataSourceWithTerm(): void
    {
        $client     = $this->makeClient();
        $dataSource = $this->makeBuilder()
            ->withFullTextSearch('my term')
            ->create($client);

        self::assertInstanceOf(DataSource::class, $dataSource);

        $withoutFts = $dataSource->withoutFullTextSearch();
        self::assertNotSame($dataSource, $withoutFts);
    }

    public function testCreateWithRawQuerySearchBuildsDataSourceWithPayload(): void
    {
        $rawQuery   = '{"query":{"match_all":{}}}';
        $client     = $this->makeClient();
        $dataSource = $this->makeBuilder()
            ->withRawQuerySearch($rawQuery)
            ->create($client);

        self::assertInstanceOf(DataSource::class, $dataSource);
    }

    // -------------------------------------------------------------------------
    // handleRequest throws
    // -------------------------------------------------------------------------

    public function testHandleRequestThrowsLogicException(): void
    {
        $this->expectException(LogicException::class);

        $this->makeBuilder()->handleRequest(new stdClass());
    }

    // -------------------------------------------------------------------------
    // withData via builder – default strategy is 9x
    // -------------------------------------------------------------------------

    public function testDefaultQueryStrategyIs9x(): void
    {
        $client = $this->makeClient();

        $dataSource = $this->makeBuilder()->create($client);

        self::assertInstanceOf(DataSource::class, $dataSource);
    }
}
