<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests;

use Kraz\ReadModel\CursorReadResponse;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Base64JsonCursorCodec;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Pagination\Cursor\SignedCursorCodec;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModelElasticSearch\DataSource;
use Kraz\ReadModelElasticSearch\DataSourceBuilder;
use Kraz\ReadModelElasticSearch\Pagination\ElasticSearchCursorPaginator;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy1x;
use Kraz\ReadModelElasticSearch\Tests\Fixtures\FakeElasticClient;
use LogicException;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

use function array_column;
use function array_map;
use function iterator_to_array;
use function substr;

#[CoversClass(DataSource::class)]
#[CoversClass(ElasticSearchCursorPaginator::class)]
final class DataSourceCursorTest extends TestCase
{
    /** @return list<array<string, mixed>> */
    private function seed(): array
    {
        // Seven rows so several page-boundary scenarios are visible.
        return [
            ['id' => 1, 'name' => 'Anna',  'department' => 'eng',     'age' => 20],
            ['id' => 2, 'name' => 'Bob',   'department' => 'eng',     'age' => 25],
            ['id' => 3, 'name' => 'Carol', 'department' => 'sales',   'age' => 30],
            ['id' => 4, 'name' => 'Dan',   'department' => 'sales',   'age' => 35],
            ['id' => 5, 'name' => 'Eve',   'department' => 'support', 'age' => 40],
            ['id' => 6, 'name' => 'Frank', 'department' => 'support', 'age' => 45],
            ['id' => 7, 'name' => 'Gina',  'department' => 'support', 'age' => 50],
        ];
    }

    /** @return DataSource<array<string, mixed>> */
    private function dataSource(FakeElasticClient|null $client = null): DataSource
    {
        $client ??= new FakeElasticClient($this->seed());

        /** @var DataSource<array<string, mixed>> $ds */
        $ds = new DataSourceBuilder()
            ->withRootIdentifier('id')
            ->create($client);

        return $ds;
    }

    /**
     * @param iterable<array<string, mixed>> $rows
     *
     * @return list<int>
     */
    private function ids(iterable $rows): array
    {
        $ids = [];
        foreach ($rows as $row) {
            $ids[] = (int) ($row['id'] ?? 0);
        }

        return $ids;
    }

    public function testFirstPageFetchesViaSearchAfter(): void
    {
        $ds = $this->dataSource()->withCursor(null, 3);

        self::assertTrue($ds->isCursored());
        self::assertFalse($ds->isPaginated());

        $paginator = $ds->cursorPaginator();
        self::assertNotNull($paginator);
        self::assertSame([1, 2, 3], $this->ids($paginator->getIterator()));
        self::assertTrue($paginator->hasNext());
        self::assertFalse($paginator->hasPrevious());
        self::assertNotNull($paginator->getNextCursor());
        self::assertNull($paginator->getPreviousCursor());
    }

    public function testFirstPageRequestDisablesTotalHitsTracking(): void
    {
        $client = new FakeElasticClient($this->seed());
        $this->dataSource($client)->withCursor(null, 3)->cursorPaginator();

        $params = $client->getLastSearchParams();
        self::assertNotNull($params);
        self::assertSame(false, $params['track_total_hits'] ?? null);
        self::assertSame(4, $params['size'] ?? null);
        self::assertArrayNotHasKey('search_after', $params);
    }

    public function testWalkForwardEnumeratesAllRowsInOrder(): void
    {
        $ds    = $this->dataSource();
        $token = null;
        $pages = [];

        for ($i = 0; $i < 10; $i++) {
            $page      = $ds->withCursor($token, 3);
            $paginator = $page->cursorPaginator();
            self::assertNotNull($paginator);

            $pages[] = $this->ids($paginator->getIterator());
            $token   = $paginator->getNextCursor();
            if ($token === null) {
                break;
            }
        }

        self::assertSame([[1, 2, 3], [4, 5, 6], [7]], $pages);
    }

    public function testBackwardNavigationReconstructsPreviousWindow(): void
    {
        $ds    = $this->dataSource();
        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $nextToken = $first->getNextCursor();
        self::assertNotNull($nextToken);

        $second = $ds->withCursor($nextToken, 3)->cursorPaginator();
        self::assertNotNull($second);
        self::assertSame([4, 5, 6], $this->ids($second->getIterator()));

        $prevToken = $second->getPreviousCursor();
        self::assertNotNull($prevToken);

        $back = $ds->withCursor($prevToken, 3)->cursorPaginator();
        self::assertNotNull($back);
        // Caller sees natural order regardless of direction.
        self::assertSame([1, 2, 3], $this->ids($back->getIterator()));
        self::assertTrue($back->hasNext());
        self::assertFalse($back->hasPrevious());
    }

    public function testCursorRespectsCustomSortAndTieBreaker(): void
    {
        $ds = $this->dataSource()
            ->withQueryExpression(QueryExpression::create()->sortBy('department', SortExpression::DIR_ASC))
            ->withCursor(null, 4);

        $paginator = $ds->cursorPaginator();
        self::assertNotNull($paginator);

        $rows = iterator_to_array($paginator->getIterator(), false);
        // Sorted by department then id (the tiebreaker the adapter injects).
        self::assertSame(['eng', 'eng', 'sales', 'sales'], array_column(
            array_map(static fn (array $r): array => ['department' => $r['department']], $rows),
            'department',
        ));
        self::assertSame([1, 2, 3, 4], $this->ids($rows));
    }

    public function testCursorClearsAndRestoresOnSwitchingModes(): void
    {
        $ds = $this->dataSource();

        $cursored = $ds->withCursor(null, 2);
        self::assertTrue($cursored->isCursored());
        self::assertNull($cursored->paginator());

        $paged = $cursored->withPagination(1, 2);
        self::assertFalse($paged->isCursored());
        self::assertTrue($paged->isPaginated());
        self::assertNotNull($paged->paginator());
        self::assertNull($paged->cursorPaginator());
    }

    public function testGetResultReturnsCursorReadResponse(): void
    {
        $result = $this->dataSource()->withCursor(null, 2)->getResult();

        self::assertInstanceOf(CursorReadResponse::class, $result);
        self::assertSame([1, 2], $this->ids($result->data ?? []));
        self::assertNotNull($result->nextCursor);
        self::assertTrue($result->hasNext);
        self::assertFalse($result->hasPrevious);
        // totalItems is intentionally null in cursor mode by default (keyset-friendly).
        self::assertNull($result->totalItems);
    }

    public function testCursorWithMismatchedSortSignatureIsRejected(): void
    {
        $codec = new Base64JsonCursorCodec();
        // Forge a cursor whose sort signature does not match the current effective sort.
        $foreignSort = SortExpression::create()->desc('name')->asc('id');
        $bad         = $codec->encode(new Cursor(
            Direction::FORWARD,
            [['field' => 'name', 'value' => 'zzz'], ['field' => 'id', 'value' => 0]],
            Cursor::signatureFor($foreignSort),
        ));

        $ds = $this->dataSource()
            ->withQueryExpression(QueryExpression::create()->sortBy('age', SortExpression::DIR_ASC))
            ->withCursor($bad, 3);

        $this->expectException(InvalidCursorException::class);
        $ds->cursorPaginator();
    }

    public function testSignedCodecCanRoundTripThroughDataSource(): void
    {
        $codec = new SignedCursorCodec(new Base64JsonCursorCodec(), 'integration-secret');
        $ds    = $this->dataSource()->withCursorCodec($codec);

        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $nextToken = $first->getNextCursor();
        self::assertNotNull($nextToken);

        $second = $ds->withCursor($nextToken, 3)->cursorPaginator();
        self::assertNotNull($second);
        self::assertSame([4, 5, 6], $this->ids($second->getIterator()));
    }

    public function testTamperedSignedCursorIsRejected(): void
    {
        $codec = new SignedCursorCodec(new Base64JsonCursorCodec(), 'integration-secret');
        $ds    = $this->dataSource()->withCursorCodec($codec);

        $first = $ds->withCursor(null, 3)->cursorPaginator();
        self::assertNotNull($first);
        $token = $first->getNextCursor();
        self::assertNotNull($token);

        // Flip one byte in the payload — signature must catch it.
        $tampered = ($token[0] === 'a' ? 'b' : 'a') . substr($token, 1);

        $this->expectException(InvalidCursorException::class);
        $ds->withCursor($tampered, 3)->cursorPaginator();
    }

    public function testItemNormalizerIsAppliedToCursorWindow(): void
    {
        $ds = $this->dataSource()
            ->withItemNormalizer(static fn (array $item): int => (int) $item['id'])
            ->withCursor(null, 3);

        $paginator = $ds->cursorPaginator();
        self::assertNotNull($paginator);
        self::assertSame([1, 2, 3], iterator_to_array($paginator->getIterator(), false));
    }

    public function testCursorPaginatorIsCachedPerInstance(): void
    {
        $ds = $this->dataSource()->withCursor(null, 3);

        self::assertSame($ds->cursorPaginator(), $ds->cursorPaginator());
    }

    public function testCursorPaginationRejectedFor1xStrategy(): void
    {
        $client = new FakeElasticClient($this->seed());
        $ds     = new DataSourceBuilder()
            ->withRootIdentifier('id')
            ->withQueryStrategy(new QueryStrategy1x())
            ->create($client);

        $this->expectException(LogicException::class);
        $ds->withCursor(null, 3)->cursorPaginator();
    }

    public function testCursorPaginationRejectedWhenCombinedWithRawQuerySearch(): void
    {
        // withCursor() does not clear rawQuerySearchPayload, so the two can collide if
        // the caller layers them in this order — the cursor path must refuse rather
        // than rewrite an opaque user-supplied body.
        $ds = $this->dataSource()
            ->withRawQuerySearch('{"query":{"match_all":{}}}')
            ->withCursor(null, 3);

        $this->expectException(LogicException::class);
        $ds->cursorPaginator();
    }
}
