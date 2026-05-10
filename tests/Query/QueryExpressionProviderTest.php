<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Query;

use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadModelDescriptor;
use Kraz\ReadModel\ReadModelDescriptorFactory;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelElasticSearch\Query\QueryExpressionProvider;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(QueryExpressionProvider::class)]
final class QueryExpressionProviderTest extends TestCase
{
    private function provider(): QueryExpressionProvider
    {
        return new QueryExpressionProvider(new ReadModelDescriptorFactory(), new QueryStrategy9x());
    }

    // -------------------------------------------------------------------------
    // apply() basics
    // -------------------------------------------------------------------------

    public function testApplyEmptyQueryReturnsMatchAll(): void
    {
        $result = $this->provider()->apply([], QueryExpression::create());

        self::assertArrayHasKey('query', $result);
        self::assertArrayHasKey('match_all', $result['query']);
    }

    public function testApplyMergesInitialDataWithQueryResult(): void
    {
        $initial = ['index' => 'my_index', 'from' => 0];
        $result  = $this->provider()->apply($initial, QueryExpression::create());

        self::assertSame('my_index', $result['index']);
        self::assertSame(0, $result['from']);
        self::assertArrayHasKey('query', $result);
    }

    public function testApplyQueryResultOverridesInitialData(): void
    {
        $initial = ['query' => ['term' => ['x' => 'stale']]];
        $filter  = FilterExpression::create()->equalTo('status', 'active', false);
        $qry     = QueryExpression::create()->andWhere($filter);

        $result = $this->provider()->apply($initial, $qry);

        self::assertSame(['term' => ['status' => 'active']], $result['query']);
    }

    // -------------------------------------------------------------------------
    // getIndexMappingFn laziness
    // -------------------------------------------------------------------------

    public function testGetIndexMappingFnIsNotCalledForEmptyQuery(): void
    {
        $called = false;

        $this->provider()->apply([], QueryExpression::create(), null, [
            'getIndexMappingFn' => static function () use (&$called): array {
                $called = true;

                return [];
            },
        ]);

        self::assertFalse($called);
    }

    public function testGetIndexMappingFnIsCalledWhenFilterIsPresent(): void
    {
        $called = false;
        $filter = FilterExpression::create()->equalTo('name', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $this->provider()->apply([], $qry, null, [
            'getIndexMappingFn' => static function () use (&$called): array {
                $called = true;

                return [];
            },
        ]);

        self::assertTrue($called);
    }

    public function testGetIndexMappingFnIsCalledWhenSortIsPresent(): void
    {
        $called = false;
        $qry    = QueryExpression::create()->sortBy('name');

        $this->provider()->apply([], $qry, null, [
            'getIndexMappingFn' => static function () use (&$called): array {
                $called = true;

                return [];
            },
        ]);

        self::assertTrue($called);
    }

    // -------------------------------------------------------------------------
    // fullTextSearchTerm option
    // -------------------------------------------------------------------------

    public function testFullTextSearchTermOptionBuildsMatchQuery(): void
    {
        $result = $this->provider()->apply([], QueryExpression::create(), null, [
            'getIndexMappingFn' => static fn (): array => [],
            'fullTextSearchTerm' => 'hello world',
        ]);

        self::assertSame([
            'match' => ['catch_all' => ['operator' => 'and', 'query' => 'hello world']],
        ], $result['query']);
    }

    // -------------------------------------------------------------------------
    // Field mapping from setFieldMapping
    // -------------------------------------------------------------------------

    public function testSetFieldMappingIsAppliedToFilterFields(): void
    {
        $provider = $this->provider();
        $provider->setFieldMapping(['displayName' => 'name']);

        $filter = FilterExpression::create()->equalTo('displayName', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $result = $provider->apply([], $qry, null, [
            'getIndexMappingFn' => static fn (): array => [],
        ]);

        self::assertSame(['term' => ['name' => 'Alice']], $result['query']);
    }

    public function testSetFieldMappingHasPriorityOverDescriptorFieldMap(): void
    {
        $provider = $this->provider();
        $provider->setFieldMapping(['label' => 'name_from_mapping']);

        $descriptor = new ReadModelDescriptor([], [], [], ['label' => 'name_from_descriptor']);

        $filter = FilterExpression::create()->equalTo('label', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $result = $provider->apply([], $qry, $descriptor, [
            'getIndexMappingFn' => static fn (): array => [],
        ]);

        self::assertSame(['term' => ['name_from_mapping' => 'Alice']], $result['query']);
    }

    // -------------------------------------------------------------------------
    // Field mapping from descriptor
    // -------------------------------------------------------------------------

    public function testDescriptorFieldMapIsUsedWhenNoExplicitMapping(): void
    {
        $descriptor = new ReadModelDescriptor([], [], [], ['label' => 'name']);

        $filter = FilterExpression::create()->equalTo('label', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $result = $this->provider()->apply([], $qry, $descriptor, [
            'getIndexMappingFn' => static fn (): array => [],
        ]);

        self::assertSame(['term' => ['name' => 'Alice']], $result['query']);
    }

    // -------------------------------------------------------------------------
    // rootIdentifier
    // -------------------------------------------------------------------------

    public function testGetRootIdentifierReturnsConfiguredValue(): void
    {
        $provider = $this->provider();
        $provider->setRootIdentifier('orderId');

        self::assertSame('orderId', $provider->getRootIdentifier());
    }

    public function testRequireSingleRootIdentifierReturnsStringIdentifier(): void
    {
        $provider = $this->provider();
        $provider->setRootIdentifier('uuid');

        self::assertSame('uuid', $provider->requireSingleRootIdentifier());
    }

    public function testRequireSingleRootIdentifierReturnsSingleArrayElement(): void
    {
        $provider = $this->provider();
        $provider->setRootIdentifier(['uuid']);

        self::assertSame('uuid', $provider->requireSingleRootIdentifier());
    }

    public function testRequireSingleRootIdentifierThrowsForCompositeIdentifier(): void
    {
        $provider = $this->provider();
        $provider->setRootIdentifier(['id', 'tenant']);

        $this->expectException(LogicException::class);

        $provider->requireSingleRootIdentifier();
    }

    // -------------------------------------------------------------------------
    // rootAlias
    // -------------------------------------------------------------------------

    public function testGetRootAliasReturnsConfiguredValue(): void
    {
        $provider = $this->provider();
        $provider->setRootAlias('o');

        self::assertSame('o', $provider->getRootAlias());
    }

    // -------------------------------------------------------------------------
    // getQueryStrategy
    // -------------------------------------------------------------------------

    public function testGetQueryStrategyReturnsInjectedStrategy(): void
    {
        $strategy = new QueryStrategy9x('search_all');
        $provider = new QueryExpressionProvider(new ReadModelDescriptorFactory(), $strategy);

        self::assertSame($strategy, $provider->getQueryStrategy());
    }

    // -------------------------------------------------------------------------
    // Descriptor from options
    // -------------------------------------------------------------------------

    public function testStringDescriptorOptionDelegatesToFactory(): void
    {
        $descriptor = new ReadModelDescriptor([], [], [], ['label' => 'name']);

        $factory = $this->createMock(ReadModelDescriptorFactoryInterface::class);
        $factory->expects(self::once())
            ->method('createReadModelDescriptorFrom')
            ->with('SomeClass')
            ->willReturn($descriptor);

        $provider = new QueryExpressionProvider($factory, new QueryStrategy9x());

        $filter = FilterExpression::create()->equalTo('label', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $provider->apply([], $qry, null, [
            'read_model_descriptor' => 'SomeClass',
            'getIndexMappingFn'     => static fn (): array => [],
        ]);
    }

    public function testExplicitDescriptorParameterWinsOverOptionString(): void
    {
        $factory = $this->createMock(ReadModelDescriptorFactoryInterface::class);
        $factory->expects(self::never())->method('createReadModelDescriptorFrom');

        $provider = new QueryExpressionProvider($factory, new QueryStrategy9x());

        $explicit   = new ReadModelDescriptor([], [], [], ['label' => 'name']);
        $filter     = FilterExpression::create()->equalTo('label', 'Alice', false);
        $qry        = QueryExpression::create()->andWhere($filter);

        $provider->apply([], $qry, $explicit, [
            'read_model_descriptor' => 'ShouldNotBeUsed',
            'getIndexMappingFn'     => static fn (): array => [],
        ]);
    }
}
