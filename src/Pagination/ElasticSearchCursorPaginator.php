<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Pagination;

use ArrayIterator;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\CursorCodecInterface;
use Kraz\ReadModel\Pagination\Cursor\CursorPaginatorInterface;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Query\SortExpression;
use LogicException;
use Override;
use ReturnTypeWillChange;
use Traversable;

use function array_reverse;
use function array_slice;
use function array_values;
use function count;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

/**
 * Cursor paginator over Elasticsearch-fetched results.
 *
 * Receives the hits already retrieved by ES — fetched with `search_after` pre-applied
 * (or no anchor on the first page) and `size = limit + 1` — together with the per-hit
 * `sort` values returned by ES. The extra (n+1)-th hit is used purely to detect whether
 * more data exists in the traversal direction; it never reaches the caller.
 *
 * For backward traversal, the search is executed against an inverted sort so ES can use
 * its sort indexes in the natural direction; this paginator reverses the returned rows
 * back to the caller-visible order before exposing them.
 *
 * Unlike the Doctrine paginator, this class never inspects the row contents to extract
 * the cursor position — ES already returns the exact sort values it used (via
 * `hit.sort`), which round-trip cleanly even when sort fields differ from `_source`
 * (e.g. `name.keyword` vs. `name`).
 *
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @phpstan-implements CursorPaginatorInterface<T>
 */
final class ElasticSearchCursorPaginator implements CursorPaginatorInterface
{
    /** @phpstan-var list<T> */
    private array $window;

    private bool $hasNext;

    private bool $hasPrevious;

    private string|null $nextCursor = null;

    private string|null $previousCursor = null;

    /**
     * @phpstan-param list<T>                   $fetched     Rows from ES, up to limit+1.
     * @phpstan-param list<list<mixed>|null>    $sortValues  Per-row `hit.sort` arrays, parallel to $fetched.
     * @phpstan-param int<1, max>               $limit
     * @phpstan-param int<0, max>|null          $totalItems
     */
    public function __construct(
        array $fetched,
        array $sortValues,
        private readonly SortExpression $effectiveSort,
        private readonly Direction $direction,
        private readonly int $limit,
        private readonly CursorCodecInterface $codec,
        bool $cameFromCursor,
        private readonly int|null $totalItems = null,
    ) {
        if ($limit < 1) {
            throw new LogicException('Cursor limit must be a positive integer.');
        }

        if ($effectiveSort->isSortEmpty()) {
            throw new LogicException('Cursor pagination requires a non-empty sort expression.');
        }

        if (count($fetched) !== count($sortValues)) {
            throw new LogicException('Cursor paginator received misaligned rows and sort values.');
        }

        $hasMore     = count($fetched) > $limit;
        $window      = $hasMore ? array_slice($fetched, 0, $limit) : $fetched;
        $windowSorts = $hasMore ? array_slice($sortValues, 0, $limit) : $sortValues;

        if ($direction === Direction::BACKWARD) {
            // The query ran under inverted sort to walk backwards efficiently; restore
            // the caller-visible order before exposing the window.
            $window      = array_reverse($window);
            $windowSorts = array_reverse($windowSorts);
        }

        $this->window = array_values($window);

        if ($direction === Direction::FORWARD) {
            $this->hasNext     = $hasMore;
            $this->hasPrevious = $cameFromCursor;
        } else {
            $this->hasPrevious = $hasMore;
            // The caller arrived here by going BACKWARD from somewhere — that somewhere
            // is always "forward" relative to the current window.
            $this->hasNext = true;
        }

        $windowCount = count($this->window);
        if ($windowCount === 0) {
            return;
        }

        $signature = Cursor::signatureFor($this->effectiveSort);
        $sortItems = array_values($this->effectiveSort->items());

        if ($this->hasNext) {
            $lastSort         = $windowSorts[$windowCount - 1] ?? null;
            $this->nextCursor = $this->codec->encode(new Cursor(
                Direction::FORWARD,
                $this->buildPosition($lastSort, $sortItems),
                $signature,
            ));
        }

        if (! $this->hasPrevious) {
            return;
        }

        $firstSort            = $windowSorts[0] ?? null;
        $this->previousCursor = $this->codec->encode(new Cursor(
            Direction::BACKWARD,
            $this->buildPosition($firstSort, $sortItems),
            $signature,
        ));
    }

    #[Override]
    public function getLimit(): int
    {
        return $this->limit;
    }

    #[Override]
    public function getDirection(): Direction
    {
        return $this->direction;
    }

    #[Override]
    public function hasNext(): bool
    {
        return $this->hasNext;
    }

    #[Override]
    public function hasPrevious(): bool
    {
        return $this->hasPrevious;
    }

    #[Override]
    public function getNextCursor(): string|null
    {
        return $this->nextCursor;
    }

    #[Override]
    public function getPreviousCursor(): string|null
    {
        return $this->previousCursor;
    }

    #[Override]
    public function getTotalItems(): int|null
    {
        return $this->totalItems;
    }

    /** @return Traversable<array-key, T> */
    #[ReturnTypeWillChange]
    #[Override]
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->window);
    }

    #[Override]
    public function count(): int
    {
        return count($this->window);
    }

    /**
     * @phpstan-param list<mixed>|null                         $hitSort
     * @phpstan-param list<array{field: string, dir: string}>  $sortItems
     *
     * @phpstan-return list<array{field: string, value: scalar|null}>
     */
    private function buildPosition(array|null $hitSort, array $sortItems): array
    {
        if ($hitSort === null || count($hitSort) < count($sortItems)) {
            throw new InvalidCursorException('Elasticsearch did not return sort values for the cursor anchor row.');
        }

        $position = [];
        foreach ($sortItems as $i => $sortItem) {
            /** @phpstan-var mixed $value */
            $value = $hitSort[$i];
            if ($value !== null && ! is_int($value) && ! is_float($value) && ! is_string($value) && ! is_bool($value)) {
                throw new InvalidCursorException('Field "' . $sortItem['field'] . '" produced a non-scalar value when extracting the cursor position.');
            }

            $position[] = ['field' => $sortItem['field'], 'value' => $value];
        }

        return $position;
    }
}
