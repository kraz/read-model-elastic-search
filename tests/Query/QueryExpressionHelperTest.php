<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Query;

use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModelElasticSearch\Query\QueryExpressionHelper;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;

#[CoversClass(QueryExpressionHelper::class)]
final class QueryExpressionHelperTest extends TestCase
{
    /** @param array<string, mixed> $indexMapping */
    private function buildQuery(FilterExpression $filter, array $indexMapping = []): mixed
    {
        $qry = QueryExpression::create()->andWhere($filter);

        return QueryExpressionHelper::create($indexMapping, new QueryStrategy9x())
            ->apply($qry, null, 'id', [], QueryExpressionProviderInterface::INCLUDE_DATA_ALL)['query'];
    }

    /** @param array<string, mixed> $indexMapping */
    private function helper(array $indexMapping = []): QueryExpressionHelper
    {
        return QueryExpressionHelper::create($indexMapping, new QueryStrategy9x());
    }

    // -------------------------------------------------------------------------
    // Empty expression
    // -------------------------------------------------------------------------

    public function testEmptyQueryExpressionBuildsMatchAll(): void
    {
        $result = $this->helper()->apply(
            QueryExpression::create(),
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        );

        self::assertInstanceOf(stdClass::class, $result['query']['match_all']);
    }

    // -------------------------------------------------------------------------
    // OP_EQ
    // -------------------------------------------------------------------------

    public function testEqualToWithIgnoreCaseOnNonKeywordBuildsMatchQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->equalTo('name', 'Alice'));

        self::assertSame(['match' => ['name' => ['query' => 'Alice', 'operator' => 'and']]], $result);
    }

    public function testEqualToOnKeywordFieldUsesTermQuery(): void
    {
        $result = $this->buildQuery(
            FilterExpression::create()->equalTo('status', 'active'),
            ['status' => ['type' => 'keyword']],
        );

        self::assertSame(['term' => ['status' => 'active']], $result);
    }

    public function testEqualToCaseSensitiveUsesTermQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->equalTo('name', 'Alice', false));

        self::assertSame(['term' => ['name' => 'Alice']], $result);
    }

    // -------------------------------------------------------------------------
    // OP_NEQ
    // -------------------------------------------------------------------------

    public function testNotEqualToWithIgnoreCaseBuildsBoolMustNotMatchQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->notEqualTo('name', 'Alice'));

        self::assertSame([
            'bool' => ['must_not' => [['match' => ['name' => ['query' => 'Alice', 'operator' => 'and']]]]],
        ], $result);
    }

    public function testNotEqualToCaseSensitiveBuildsBoolMustNotTermQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->notEqualTo('name', 'Alice', false));

        self::assertSame([
            'bool' => ['must_not' => [['term' => ['name' => 'Alice']]]],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // OP_IS_NULL / OP_IS_NOT_NULL
    // -------------------------------------------------------------------------

    public function testIsNullBuildsMustNotExistsQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->isNull('email'));

        self::assertSame([
            'bool' => ['must_not' => [['exists' => ['field' => 'email']]]],
        ], $result);
    }

    public function testIsNotNullBuildsExistsQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->isNotNull('email'));

        self::assertSame(['exists' => ['field' => 'email']], $result);
    }

    // -------------------------------------------------------------------------
    // Range operators
    // -------------------------------------------------------------------------

    public function testLowerThanBuildsRangeLtQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->lowerThan('age', '30'));

        self::assertSame(['range' => ['age' => ['lt' => '30']]], $result);
    }

    public function testLowerThanOrEqualBuildsRangeLteQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->lowerThanOrEqual('age', '30'));

        self::assertSame(['range' => ['age' => ['lte' => '30']]], $result);
    }

    public function testGreaterThanBuildsRangeGtQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->greaterThan('age', '18'));

        self::assertSame(['range' => ['age' => ['gt' => '18']]], $result);
    }

    public function testGreaterThanOrEqualBuildsRangeGteQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->greaterThanOrEqual('age', '18'));

        self::assertSame(['range' => ['age' => ['gte' => '18']]], $result);
    }

    // -------------------------------------------------------------------------
    // OP_STARTS_WITH / OP_DOES_NOT_START_WITH
    // -------------------------------------------------------------------------

    public function testStartsWithCaseInsensitiveBuildsWildcardWithCaseInsensitiveFlag(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->startsWith('name', 'Al'));

        self::assertSame([
            'wildcard' => ['name' => ['value' => 'al*', 'case_insensitive' => true]],
        ], $result);
    }

    public function testStartsWithCaseSensitiveBuildsPrefixQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->startsWith('name', 'Al', false));

        self::assertSame(['prefix' => ['name' => 'Al']], $result);
    }

    public function testDoesNotStartWithCaseInsensitiveBuildsMustNotWildcard(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->doesNotStartWith('name', 'Al'));

        self::assertSame([
            'bool' => ['must_not' => [['wildcard' => ['name' => ['value' => 'al*', 'case_insensitive' => true]]]]],
        ], $result);
    }

    public function testDoesNotStartWithCaseSensitiveBuildsMustNotPrefix(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->doesNotStartWith('name', 'Al', false));

        self::assertSame([
            'bool' => ['must_not' => [['prefix' => ['name' => 'Al']]]],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // OP_ENDS_WITH / OP_DOES_NOT_END_WITH
    // -------------------------------------------------------------------------

    public function testEndsWithCaseInsensitiveBuildsWildcardWithCaseInsensitiveFlag(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->endsWith('name', 'ice'));

        self::assertSame([
            'wildcard' => ['name' => ['value' => '*ice', 'case_insensitive' => true]],
        ], $result);
    }

    public function testEndsWithCaseSensitiveBuildsWildcard(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->endsWith('name', 'ice', false));

        self::assertSame(['wildcard' => ['name' => '*ice']], $result);
    }

    public function testDoesNotEndWithCaseInsensitiveBuildsMustNotWildcard(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->doesNotEndWith('name', 'ice'));

        self::assertSame([
            'bool' => ['must_not' => [['wildcard' => ['name' => ['value' => '*ice', 'case_insensitive' => true]]]]],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // OP_CONTAINS / OP_DOES_NOT_CONTAIN
    // -------------------------------------------------------------------------

    public function testContainsCaseInsensitiveBuildsWildcardWithStars(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->contains('name', 'Ali'));

        self::assertSame([
            'wildcard' => ['name' => ['value' => '*ali*', 'case_insensitive' => true]],
        ], $result);
    }

    public function testContainsCaseSensitiveBuildsWildcardWithoutFlag(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->contains('name', 'Ali', false));

        self::assertSame(['wildcard' => ['name' => '*Ali*']], $result);
    }

    public function testDoesNotContainCaseInsensitiveBuildsMustNotWildcard(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->doesNotContain('name', 'Ali'));

        self::assertSame([
            'bool' => ['must_not' => [['wildcard' => ['name' => ['value' => '*ali*', 'case_insensitive' => true]]]]],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // OP_IS_EMPTY / OP_IS_NOT_EMPTY
    // -------------------------------------------------------------------------

    public function testIsEmptyBuildsShouldWithMissingOrEmpty(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->isEmpty('email'));

        self::assertSame([
            'bool' => [
                'should' => [
                    ['bool' => ['must_not' => [['exists' => ['field' => 'email']]]]],
                    ['term' => ['email' => '']],
                ],
                'minimum_should_match' => 1,
            ],
        ], $result);
    }

    public function testIsNotEmptyBuildsExistsMustNotEmpty(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->isNotEmpty('email'));

        self::assertSame([
            'bool' => [
                'must' => [['exists' => ['field' => 'email']]],
                'must_not' => [['term' => ['email' => '']]],
            ],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // OP_IN_LIST / OP_NOT_IN_LIST
    // -------------------------------------------------------------------------

    public function testInListOnNonTextFieldBuildsTermsQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->inList('status', ['active', 'pending']));

        self::assertSame(['terms' => ['status' => ['active', 'pending']]], $result);
    }

    public function testInListOnTextFieldBuildsShouldMatchQueries(): void
    {
        $result = $this->buildQuery(
            FilterExpression::create()->inList('tags', ['php', 'java']),
            ['tags' => ['type' => 'text']],
        );

        self::assertSame([
            'bool' => [
                'should' => [
                    ['match' => ['tags' => ['query' => 'php', 'operator' => 'and']]],
                    ['match' => ['tags' => ['query' => 'java', 'operator' => 'and']]],
                ],
                'minimum_should_match' => 1,
            ],
        ], $result);
    }

    public function testNotInListOnNonTextFieldBuildsMustNotTermsQuery(): void
    {
        $result = $this->buildQuery(FilterExpression::create()->notInList('status', ['active', 'pending']));

        self::assertSame([
            'bool' => ['must_not' => [['terms' => ['status' => ['active', 'pending']]]]],
        ], $result);
    }

    public function testNotInListOnTextFieldBuildsMustNotShouldMatch(): void
    {
        $result = $this->buildQuery(
            FilterExpression::create()->notInList('tags', ['php', 'java']),
            ['tags' => ['type' => 'text']],
        );

        self::assertSame([
            'bool' => [
                'must_not' => [
                    [
                        'bool' => [
                            'should' => [
                                ['match' => ['tags' => ['query' => 'php', 'operator' => 'and']]],
                                ['match' => ['tags' => ['query' => 'java', 'operator' => 'and']]],
                            ],
                            'minimum_should_match' => 1,
                        ],
                    ],
                ],
            ],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // not() modifier on a filter
    // -------------------------------------------------------------------------

    public function testNotModifierOnEqualToWrapsInMustNot(): void
    {
        $filter = FilterExpression::create()->not(FilterExpression::create()->equalTo('name', 'Alice', false));
        $result = $this->buildQuery($filter);

        self::assertSame([
            'bool' => ['must_not' => [['term' => ['name' => 'Alice']]]],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // Multiple filters (AND / OR logic)
    // -------------------------------------------------------------------------

    public function testMultipleFiltersWithAndLogicBuildsBoolMust(): void
    {
        $qry = QueryExpression::create()->andWhere(
            FilterExpression::create()->equalTo('status', 'active', false),
            FilterExpression::create()->greaterThan('age', '18'),
        );

        $result = $this->helper()->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame([
            'bool' => [
                'must' => [
                    ['term' => ['status' => 'active']],
                    ['range' => ['age' => ['gt' => '18']]],
                ],
            ],
        ], $result);
    }

    public function testMultipleFiltersWithOrLogicBuildsBoolShould(): void
    {
        $qry = QueryExpression::create()->orWhere(
            FilterExpression::create()->equalTo('status', 'active', false),
            FilterExpression::create()->equalTo('status', 'pending', false),
        );

        $result = $this->helper()->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame([
            'bool' => [
                'should' => [
                    ['term' => ['status' => 'active']],
                    ['term' => ['status' => 'pending']],
                ],
                'minimum_should_match' => 1,
            ],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // Nested field handling
    // -------------------------------------------------------------------------

    public function testSingleNestedFieldFilterIsWrappedInNestedQuery(): void
    {
        $indexMapping = ['items.name' => ['type' => 'text', 'nestedPath' => 'items']];
        $result       = $this->buildQuery(
            FilterExpression::create()->equalTo('items.name', 'Widget'),
            $indexMapping,
        );

        self::assertSame([
            'nested' => [
                'path'  => 'items',
                'query' => ['match' => ['items.name' => ['query' => 'Widget', 'operator' => 'and']]],
            ],
        ], $result);
    }

    public function testMultipleFiltersOnSameNestedPathAreCombinedInSingleNestedQuery(): void
    {
        $indexMapping = [
            'items.name'  => ['type' => 'text', 'nestedPath' => 'items'],
            'items.price' => ['type' => 'double', 'nestedPath' => 'items'],
        ];

        $qry = QueryExpression::create()->andWhere(
            FilterExpression::create()->equalTo('items.name', 'Widget'),
            FilterExpression::create()->lowerThan('items.price', '50'),
        );

        $result = $this->helper($indexMapping)->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame([
            'nested' => [
                'path'  => 'items',
                'query' => [
                    'bool' => [
                        'must' => [
                            ['match' => ['items.name' => ['query' => 'Widget', 'operator' => 'and']]],
                            ['range' => ['items.price' => ['lt' => '50']]],
                        ],
                    ],
                ],
            ],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // Field mapping applied to filter
    // -------------------------------------------------------------------------

    public function testFieldMappingRenamesFieldInFilterQuery(): void
    {
        $filter = FilterExpression::create()->equalTo('displayName', 'Alice', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $result = $this->helper()->apply(
            $qry,
            null,
            'id',
            ['displayName' => 'name'],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame(['term' => ['name' => 'Alice']], $result);
    }

    // -------------------------------------------------------------------------
    // Sort building
    // -------------------------------------------------------------------------

    public function testSortByTextFieldAppendsKeywordSuffix(): void
    {
        $qry    = QueryExpression::create()->sortBy('name');
        $result = $this->helper(['name' => ['type' => 'text']])->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        );

        self::assertSame([
            ['name.keyword' => ['order' => 'asc', 'missing' => '_last', 'unmapped_type' => 'keyword']],
        ], $result['sort']);
    }

    public function testSortDescUsesFirstMissingValue(): void
    {
        $qry    = QueryExpression::create()->sortBy('createdAt', 'desc');
        $result = $this->helper(['createdAt' => ['type' => 'date']])->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        );

        self::assertSame('_first', $result['sort'][0]['createdAt']['missing']);
    }

    public function testSortOnNestedFieldIncludesNestedPath(): void
    {
        $qry          = QueryExpression::create()->sortBy('items.price');
        $indexMapping = ['items.price' => ['type' => 'double', 'nestedPath' => 'items']];
        $result       = $this->helper($indexMapping)->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        );

        self::assertSame([
            ['items.price' => [
                'order'         => 'asc',
                'missing'       => '_last',
                'nested'        => ['path' => 'items'],
                'unmapped_type' => 'double',
            ]],
        ], $result['sort']);
    }

    public function testSortIsNotIncludedWhenIncludeDataExcludesSort(): void
    {
        $qry    = QueryExpression::create()->sortBy('name');
        $result = $this->helper()->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_FILTER,
        );

        self::assertArrayNotHasKey('sort', $result);
    }

    // -------------------------------------------------------------------------
    // Full-text search
    // -------------------------------------------------------------------------

    public function testFullTextSearchWithoutFilterBuildsMatchQuery(): void
    {
        $result = $this->helper()->apply(
            QueryExpression::create(),
            'hello world',
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame([
            'match' => ['catch_all' => ['operator' => 'and', 'query' => 'hello world']],
        ], $result);
    }

    public function testFullTextSearchWithFilterCombinesUsingBoolMustFilter(): void
    {
        $filter = FilterExpression::create()->equalTo('status', 'active', false);
        $qry    = QueryExpression::create()->andWhere($filter);

        $result = $this->helper()->apply(
            $qry,
            'foo bar',
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame([
            'bool' => [
                'must'   => [['match' => ['catch_all' => ['operator' => 'and', 'query' => 'foo bar']]]],
                'filter' => [['term' => ['status' => 'active']]],
            ],
        ], $result);
    }

    // -------------------------------------------------------------------------
    // Values mode
    // -------------------------------------------------------------------------

    public function testValuesModeBuildsTermsQueryOnIdentifierField(): void
    {
        $qry    = QueryExpression::create()->withValues(['123', '456']);
        $result = $this->helper()->apply(
            $qry,
            null,
            'orderId',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame(['terms' => ['orderId' => ['123', '456']]], $result);
    }

    public function testValuesModeAppliesFieldMappingToIdentifier(): void
    {
        $qry    = QueryExpression::create()->withValues(['abc', 'def']);
        $result = $this->helper()->apply(
            $qry,
            null,
            'uuid',
            ['uuid' => 'id'],
            QueryExpressionProviderInterface::INCLUDE_DATA_ALL,
        )['query'];

        self::assertSame(['terms' => ['id' => ['abc', 'def']]], $result);
    }

    public function testValuesAreNotIncludedWhenIncludeDataExcludesValues(): void
    {
        $filter = FilterExpression::create()->equalTo('status', 'active', false);
        $qry    = QueryExpression::create()
            ->andWhere($filter)
            ->withValues(['1', '2']);

        $result = $this->helper()->apply(
            $qry,
            null,
            'id',
            [],
            QueryExpressionProviderInterface::INCLUDE_DATA_FILTER,
        )['query'];

        self::assertSame(['term' => ['status' => 'active']], $result);
    }

    // -------------------------------------------------------------------------
    // INCLUDE_DATA flag coverage
    // -------------------------------------------------------------------------

    public function testIncludeDataFilterOnlyDoesNotIncludeSort(): void
    {
        $qry    = QueryExpression::create()
            ->andWhere(FilterExpression::create()->equalTo('status', 'ok', false))
            ->sortBy('name');
        $result = $this->helper()->apply($qry, null, 'id', [], QueryExpressionProviderInterface::INCLUDE_DATA_FILTER);

        self::assertArrayNotHasKey('sort', $result);
        self::assertSame(['term' => ['status' => 'ok']], $result['query']);
    }

    public function testIncludeDataSortOnlyDoesNotIncludeFilter(): void
    {
        $qry    = QueryExpression::create()
            ->andWhere(FilterExpression::create()->equalTo('status', 'ok', false))
            ->sortBy('name');
        $result = $this->helper()->apply($qry, null, 'id', [], QueryExpressionProviderInterface::INCLUDE_DATA_SORT);

        self::assertInstanceOf(stdClass::class, $result['query']['match_all']);
        self::assertArrayHasKey('sort', $result);
    }
}
