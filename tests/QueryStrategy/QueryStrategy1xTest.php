<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\QueryStrategy;

use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy1x;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(QueryStrategy1x::class)]
final class QueryStrategy1xTest extends TestCase
{
    // buildFullTextSearchWithFilter

    public function testBuildFullTextSearchWithoutFilterUsesAllField(): void
    {
        $result = (new QueryStrategy1x())->buildFullTextSearchWithFilter('hello', null);

        self::assertSame([
            'match' => ['_all' => ['operator' => 'and', 'query' => 'hello']],
        ], $result);
    }

    public function testBuildFullTextSearchWithFilterUsesFilteredQuery(): void
    {
        $filter = ['term' => ['status' => 'active']];
        $result = (new QueryStrategy1x())->buildFullTextSearchWithFilter('hello', $filter);

        self::assertSame([
            'filtered' => [
                'query' => [
                    'match' => ['_all' => ['operator' => 'and', 'query' => 'hello']],
                ],
                'filter' => ['term' => ['status' => 'active']],
            ],
        ], $result);
    }

    // getSortableField

    public function testGetSortableFieldReturnsFieldAsIsForTextType(): void
    {
        self::assertSame('name', (new QueryStrategy1x())->getSortableField('name', 'text'));
    }

    public function testGetSortableFieldReturnsFieldAsIsForKeywordType(): void
    {
        self::assertSame('status', (new QueryStrategy1x())->getSortableField('status', 'keyword'));
    }

    public function testGetSortableFieldReturnsFieldAsIsForNullType(): void
    {
        self::assertSame('field', (new QueryStrategy1x())->getSortableField('field', null));
    }

    // getUnmappedType

    public function testGetUnmappedTypeForTextIsString(): void
    {
        self::assertSame('string', (new QueryStrategy1x())->getUnmappedType('text'));
    }

    public function testGetUnmappedTypeForKeywordIsString(): void
    {
        self::assertSame('string', (new QueryStrategy1x())->getUnmappedType('keyword'));
    }

    public function testGetUnmappedTypeForNullIsString(): void
    {
        self::assertSame('string', (new QueryStrategy1x())->getUnmappedType(null));
    }

    public function testGetUnmappedTypeForLongIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy1x())->getUnmappedType('long'));
    }

    public function testGetUnmappedTypeForIntegerIsLong(): void
    {
        self::assertSame('long', (new QueryStrategy1x())->getUnmappedType('integer'));
    }

    public function testGetUnmappedTypeForDoubleIsDouble(): void
    {
        self::assertSame('double', (new QueryStrategy1x())->getUnmappedType('double'));
    }

    public function testGetUnmappedTypeForDateIsDate(): void
    {
        self::assertSame('date', (new QueryStrategy1x())->getUnmappedType('date'));
    }

    public function testGetUnmappedTypeForBooleanIsBoolean(): void
    {
        self::assertSame('boolean', (new QueryStrategy1x())->getUnmappedType('boolean'));
    }

    // extractHitsTotal

    public function testExtractHitsTotalFromScalarInteger(): void
    {
        self::assertSame(15, (new QueryStrategy1x())->extractHitsTotal(['hits' => ['total' => 15]]));
    }

    public function testExtractHitsTotalReturnsZeroWhenAbsent(): void
    {
        self::assertSame(0, (new QueryStrategy1x())->extractHitsTotal([]));
    }

    // extractMappingProperties

    public function testExtractMappingPropertiesUses1xStructureWithTypeName(): void
    {
        $rawMapping = [
            'my_index' => [
                'mappings' => [
                    'my_type' => [
                        'properties' => ['name' => ['type' => 'string']],
                    ],
                ],
            ],
        ];

        self::assertSame(
            ['name' => ['type' => 'string']],
            (new QueryStrategy1x())->extractMappingProperties($rawMapping, 'my_type'),
        );
    }

    public function testExtractMappingPropertiesReturnsEmptyArrayWhenStructureAbsent(): void
    {
        self::assertSame([], (new QueryStrategy1x())->extractMappingProperties([], 'type'));
    }
}
