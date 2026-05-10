<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\QueryStrategy;

use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(QueryStrategy9x::class)]
final class QueryStrategy9xTest extends TestCase
{
    // buildFullTextSearchWithFilter

    public function testBuildFullTextSearchWithoutFilterReturnsMatchQuery(): void
    {
        $result = (new QueryStrategy9x())->buildFullTextSearchWithFilter('hello world', null);

        self::assertSame([
            'match' => ['catch_all' => ['operator' => 'and', 'query' => 'hello world']],
        ], $result);
    }

    public function testBuildFullTextSearchWithFilterReturnsBoolMustFilter(): void
    {
        $filter = ['term' => ['status' => 'active']];
        $result = (new QueryStrategy9x())->buildFullTextSearchWithFilter('hello', $filter);

        self::assertSame([
            'bool' => [
                'must' => [
                    ['match' => ['catch_all' => ['operator' => 'and', 'query' => 'hello']]],
                ],
                'filter' => [['term' => ['status' => 'active']]],
            ],
        ], $result);
    }

    public function testBuildFullTextSearchUsesCustomCatchAllField(): void
    {
        $strategy = new QueryStrategy9x('my_search');
        $result   = $strategy->buildFullTextSearchWithFilter('test', null);

        self::assertArrayHasKey('my_search', $result['match']);
    }

    // getSortableField

    public function testGetSortableFieldAppendsKeywordSuffixForTextType(): void
    {
        self::assertSame('name.keyword', (new QueryStrategy9x())->getSortableField('name', 'text'));
    }

    public function testGetSortableFieldDoesNotAppendSuffixForKeywordType(): void
    {
        self::assertSame('status', (new QueryStrategy9x())->getSortableField('status', 'keyword'));
    }

    public function testGetSortableFieldDoesNotAppendSuffixForNullType(): void
    {
        self::assertSame('someField', (new QueryStrategy9x())->getSortableField('someField', null));
    }

    public function testGetSortableFieldDoesNotAppendSuffixForDateType(): void
    {
        self::assertSame('createdAt', (new QueryStrategy9x())->getSortableField('createdAt', 'date'));
    }

    public function testGetSortableFieldDoesNotAppendSuffixForNumericType(): void
    {
        self::assertSame('price', (new QueryStrategy9x())->getSortableField('price', 'double'));
    }

    // getUnmappedType

    public function testGetUnmappedTypeForTextIsKeyword(): void
    {
        self::assertSame('keyword', (new QueryStrategy9x())->getUnmappedType('text'));
    }

    public function testGetUnmappedTypeForStringIsKeyword(): void
    {
        self::assertSame('keyword', (new QueryStrategy9x())->getUnmappedType('string'));
    }

    public function testGetUnmappedTypeForLongIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy9x())->getUnmappedType('long'));
    }

    public function testGetUnmappedTypeForIntegerIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy9x())->getUnmappedType('integer'));
    }

    public function testGetUnmappedTypeForShortIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy9x())->getUnmappedType('short'));
    }

    public function testGetUnmappedTypeForByteIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy9x())->getUnmappedType('byte'));
    }

    public function testGetUnmappedTypeForDoubleIsDouble(): void
    {
        self::assertSame('double', (new QueryStrategy9x())->getUnmappedType('double'));
    }

    public function testGetUnmappedTypeForFloatIsDouble(): void
    {
        self::assertSame('double', (new QueryStrategy9x())->getUnmappedType('float'));
    }

    public function testGetUnmappedTypeForHalfFloatIsDouble(): void
    {
        self::assertSame('double', (new QueryStrategy9x())->getUnmappedType('half_float'));
    }

    public function testGetUnmappedTypeForScaledFloatIsDouble(): void
    {
        self::assertSame('double', (new QueryStrategy9x())->getUnmappedType('scaled_float'));
    }

    public function testGetUnmappedTypeForDateIsDate(): void
    {
        self::assertSame('date', (new QueryStrategy9x())->getUnmappedType('date'));
    }

    public function testGetUnmappedTypeForBooleanIsBoolean(): void
    {
        self::assertSame('boolean', (new QueryStrategy9x())->getUnmappedType('boolean'));
    }

    public function testGetUnmappedTypeForKeywordIsKeyword(): void
    {
        self::assertSame('keyword', (new QueryStrategy9x())->getUnmappedType('keyword'));
    }

    public function testGetUnmappedTypeForNullDefaultsToKeyword(): void
    {
        self::assertSame('keyword', (new QueryStrategy9x())->getUnmappedType(null));
    }

    public function testGetUnmappedTypeForUnknownTypeDefaultsToKeyword(): void
    {
        self::assertSame('keyword', (new QueryStrategy9x())->getUnmappedType('blob'));
    }

    // extractHitsTotal

    public function testExtractHitsTotalFromScalarInteger(): void
    {
        self::assertSame(42, (new QueryStrategy9x())->extractHitsTotal(['hits' => ['total' => 42]]));
    }

    public function testExtractHitsTotalFromArrayWithValueKey(): void
    {
        self::assertSame(99, (new QueryStrategy9x())->extractHitsTotal(['hits' => ['total' => ['value' => 99]]]));
    }

    public function testExtractHitsTotalReturnsZeroWhenAbsent(): void
    {
        self::assertSame(0, (new QueryStrategy9x())->extractHitsTotal([]));
    }

    // extractMappingProperties

    public function testExtractMappingPropertiesReturnsNestedPropertiesForFirstKey(): void
    {
        $rawMapping = [
            'my_index' => [
                'mappings' => [
                    'properties' => ['title' => ['type' => 'text']],
                ],
            ],
        ];

        self::assertSame(
            ['title' => ['type' => 'text']],
            (new QueryStrategy9x())->extractMappingProperties($rawMapping, 'my_index'),
        );
    }

    public function testExtractMappingPropertiesReturnsEmptyArrayWhenStructureAbsent(): void
    {
        self::assertSame([], (new QueryStrategy9x())->extractMappingProperties([], 'any'));
    }
}
