<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use ArrayIterator;
use InvalidArgumentException;
use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ReadModel\Exception\InvalidCursorException;
use Kraz\ReadModel\Pagination\Cursor\Base64JsonCursorCodec;
use Kraz\ReadModel\Pagination\Cursor\Cursor;
use Kraz\ReadModel\Pagination\Cursor\CursorCodecInterface;
use Kraz\ReadModel\Pagination\Cursor\CursorPaginatorInterface;
use Kraz\ReadModel\Pagination\Cursor\Direction;
use Kraz\ReadModel\Pagination\InMemoryPaginator;
use Kraz\ReadModel\Pagination\PaginatorInterface;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\Query\SortExpression;
use Kraz\ReadModel\ReadDataProviderAccess;
use Kraz\ReadModel\ReadDataProviderComposition;
use Kraz\ReadModel\ReadDataProviderCompositionInterface;
use Kraz\ReadModel\ReadDataProviderInterface;
use Kraz\ReadModel\ReadDataProviderPayload;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModel\ReadResponse;
use Kraz\ReadModelElasticSearch\Pagination\ElasticSearchCursorPaginator;
use Kraz\ReadModelElasticSearch\Query\QueryExpressionProvider;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategyInterface;
use LogicException;
use Nyholm\Psr7\Factory\Psr17Factory;
use Override;
use Psr\Http\Message\RequestInterface;
use RuntimeException;
use Symfony\Bridge\PsrHttpMessage\Factory\PsrHttpFactory;
use Symfony\Component\HttpFoundation\Request as SymfonyRequest;
use Traversable;

use function array_filter;
use function array_values;
use function class_exists;
use function count;
use function is_array;
use function is_callable;
use function json_decode;
use function parse_str;
use function sprintf;

/**
 * @phpstan-template-covariant T of array<string, mixed>|object
 * @implements ReadDataProviderInterface<T>
 * @implements FullTextSearchReadModelInterface<T>
 * @implements ElasticRawQuerySearchReadModelInterface<T>
 */
class DataSource implements ReadDataProviderInterface, FullTextSearchReadModelInterface, ElasticRawQuerySearchReadModelInterface
{
    /** @use ReadDataProviderAccess<T> */
    use ReadDataProviderAccess;
    /** @use ReadDataProviderComposition<T> */
    use ReadDataProviderComposition;

    /** @phpstan-var ReadDataProviderPayload<T>|null */
    private ReadDataProviderPayload|null $payload = null;
    /** @phpstan-var PaginatorInterface<T>|null */
    private PaginatorInterface|null $paginatorInstance = null;
    /** @phpstan-var CursorPaginatorInterface<T>|null */
    private CursorPaginatorInterface|null $cursorPaginatorInstance = null;
    private string|null $fullTextSearchTerm                        = null;
    private string|null $rawQuerySearchPayload                     = null;
    private CursorCodecInterface $cursorCodec;

    /** @phpstan-param ElasticSearchClientInterface&ElasticSearchReadClientInterface<T> $client */
    public function __construct(
        private readonly ElasticSearchClientInterface&ElasticSearchReadClientInterface $client,
        private readonly string|null $index = null,
        QueryExpressionProviderInterface|null $queryExpressionProvider = null,
        private readonly QueryStrategyInterface $queryStrategy = new QueryStrategy9x(),
    ) {
        $this->queryExpressionProvider = $queryExpressionProvider;
        $this->cursorCodec             = new Base64JsonCursorCodec();
    }

    /**
     * Replace the codec used to encode and decode cursor tokens.
     *
     * Pass an instance of {@see \Kraz\ReadModel\Pagination\Cursor\SignedCursorCodec} to
     * enable HMAC-based tamper detection. The default codec is opaque but unsigned.
     *
     * @phpstan-return static<T>
     */
    public function withCursorCodec(CursorCodecInterface $codec): static
    {
        /** @phpstan-var static<T> $clone */
        $clone              = clone $this;
        $clone->cursorCodec = $codec;

        return $clone;
    }

    /** @phpstan-return ReadDataProviderPayload<T> */
    private function getPayload(): ReadDataProviderPayload
    {
        if ($this->payload !== null) {
            return $this->payload;
        }

        /** @phpstan-var ReadResponse<T> $result */
        $result = $this->client->read($this->getParams(), $this->index);

        /** @phpstan-var ReadDataProviderPayload<T> $payload */
        $payload       = new ReadDataProviderPayload($result);
        $this->payload = $payload;

        return $this->payload;
    }

    protected function createDefaultQueryExpressionProvider(ReadModelDescriptorFactoryInterface $factory): QueryExpressionProviderInterface
    {
        return new QueryExpressionProvider($factory, $this->queryStrategy);
    }

    /** @phpstan-return array<string, mixed> */
    private function getParams(): array
    {
        if ($this->rawQuerySearchPayload !== null) {
            return json_decode($this->rawQuerySearchPayload, true);
        }

        $queryExpression = $this->getWrappedQueryExpression() ?? QueryExpression::create();
        $provider        = $this->getOrCreateQueryExpressionProvider();

        $params = $provider->apply([], $queryExpression, null, [
            'getIndexMappingFn'      => fn () => $this->client->getFlattenedMapping($this->index, $this->queryStrategy->extractMappingProperties(...)),
            'fullTextSearchTerm' => $this->fullTextSearchTerm,
        ]);

        if ($this->pagination !== null) {
            [$page, $itemsPerPage] = $this->pagination;
            $params['from']        = ($page - 1) * $itemsPerPage;
            $params['size']        = $itemsPerPage;
        } elseif ($this->limit !== null) {
            [$limitValue, $offsetValue] = $this->limit;
            $params['size']             = $limitValue;
            if ($offsetValue !== null && $offsetValue > 0) {
                $params['from'] = $offsetValue;
            }
        }

        return $params;
    }

    #[Override]
    public function withQueryModifier(callable $modifier, bool $append = false): static
    {
        throw new LogicException('Query modifiers are not supported in the ElasticSearch DataSource.');
    }

    /** @phpstan-return array<int, T> */
    private function filteredItems(): array
    {
        $items = $this->getPayload()->getData();

        if (count($this->specifications) === 0) {
            return $items;
        }

        return array_values(array_filter($items, /** @phpstan-param T $item */ function (mixed $item): bool {
            foreach ($this->specifications as $specification) {
                if (! $specification->isSatisfiedBy($item)) {
                    return false;
                }
            }

            return true;
        }));
    }

    #[Override]
    public function count(): int
    {
        $this->assertNoSpecifications();

        if ($this->cursor !== null) {
            return $this->cursorPaginator()?->count() ?? 0;
        }

        $paginator = $this->paginator();
        if ($paginator !== null) {
            return $paginator->count();
        }

        return count($this->filteredItems());
    }

    #[Override]
    public function totalCount(): int
    {
        $this->assertNoSpecifications();

        if ($this->cursor !== null) {
            // Cursor mode does not pre-compute total counts (we explicitly disable
            // track_total_hits to keep pages O(limit) on the ES side). Surface 0 when
            // unknown so isEmpty() and other consumers behave sanely.
            return $this->cursorPaginator()?->getTotalItems() ?? 0;
        }

        return $this->getPayload()->getTotalItems();
    }

    #[Override]
    public function isPaginated(): bool
    {
        if (count($this->specifications) > 0) {
            return false;
        }

        if ($this->pagination === null) {
            return false;
        }

        [$page, $itemsPerPage] = $this->pagination;

        return $page > 0 && $itemsPerPage > 0;
    }

    #[Override]
    public function getIterator(): Traversable
    {
        $specifications = $this->specifications;
        $hasSpecs       = count($specifications) > 0;

        if ($hasSpecs && $this->limit === null) {
            throw new LogicException('Specifications can only be used with a limit. Call withLimit() before using withSpecification().');
        }

        if ($hasSpecs && $this->limit !== null) {
            [$limitValue, $offsetValue] = $this->limit;

            yield from $this->withoutSpecification()->withoutLimit()->specificationsIterator(
                $specifications,
                $limitValue,
                $offsetValue ?? 0,
            );

            return;
        }

        if ($this->cursor !== null) {
            $cursorPaginator = $this->cursorPaginator();
            if ($cursorPaginator !== null) {
                // Items are already normalized inside cursorPaginator() before being
                // handed to the paginator, so no further normalization is needed here.
                yield from $cursorPaginator->getIterator();

                return;
            }
        }

        $paginator = $this->paginator();
        $items     = $paginator?->getIterator() ?? new ArrayIterator($this->filteredItems());

        $itemNormalizer = $this->itemNormalizer;
        if ($itemNormalizer !== null) {
            foreach ($items as $item) {
                yield $itemNormalizer($item);
            }

            return;
        }

        yield from $items;
    }

    #[Override]
    public function paginator(): PaginatorInterface|null
    {
        $this->assertNoSpecifications();

        if ($this->paginatorInstance !== null) {
            return $this->paginatorInstance;
        }

        if ($this->pagination === null) {
            return null;
        }

        [$page, $itemsPerPage] = $this->pagination;
        $payload               = $this->getPayload();

        $this->paginatorInstance = new InMemoryPaginator(
            $payload->getIterator(),
            $payload->getTotalItems(),
            $payload->getCurrentPage() ?: $page,
            $itemsPerPage,
            0,
        );

        return $this->paginatorInstance;
    }

    #[Override]
    public function withFullTextSearch(string $term): static
    {
        /** @phpstan-var static<T> $clone */
        $clone                     = clone $this;
        $clone->fullTextSearchTerm = $term;

        return $clone;
    }

    #[Override]
    public function withoutFullTextSearch(): static
    {
        /** @phpstan-var static<T> $clone */
        $clone                     = clone $this;
        $clone->fullTextSearchTerm = null;

        return $clone;
    }

    #[Override]
    public function withRawQuerySearch(string $query): static
    {
        /** @phpstan-var static<T> $clone */
        $clone = $this
            ->withoutSpecification()
            ->withoutQueryExpression()
            ->withoutPagination()
            ->withoutCursor()
            ->withoutFullTextSearch();

        $clone->rawQuerySearchPayload = $query;

        return $clone;
    }

    #[Override]
    public function withoutRawQuerySearch(): static
    {
        /** @phpstan-var static<T> $clone */
        $clone                        = clone $this;
        $clone->rawQuerySearchPayload = null;

        return $clone;
    }

    #[Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        /** @phpstan-var static<T> $ds */
        $ds = static::applyRequestTo($this, $request, $fieldsOperator, $fieldsIgnoreCase);

        return $ds;
    }

    public function __clone()
    {
        $this->payload                 = null;
        $this->paginatorInstance       = null;
        $this->cursorPaginatorInstance = null;
    }

    /**
     * @phpstan-param J $target
     * @phpstan-param array<string, string> $fieldsOperator
     * @phpstan-param array<string, bool> $fieldsIgnoreCase
     *
     * @phpstan-return J
     *
     * @phpstan-template J of ReadDataProviderCompositionInterface<object|array<string, mixed>>
     */
    public static function applyRequestTo(ReadDataProviderCompositionInterface $target, object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): ReadDataProviderCompositionInterface
    {
        if (class_exists(SymfonyRequest::class) && $request instanceof SymfonyRequest) {
            if (! class_exists(Psr17Factory::class)) {
                throw new InvalidArgumentException('You need to install "nyholm/psr7" and "symfony/psr-http-message-bridge" in order to handle Symfony requests!');
            }

            $psr17Factory   = new Psr17Factory();
            $psrHttpFactory = new PsrHttpFactory($psr17Factory, $psr17Factory, $psr17Factory, $psr17Factory);
            $request        = $psrHttpFactory->createRequest($request);
        }

        if ($request instanceof RequestInterface) {
            parse_str($request->getUri()->getQuery(), $input);

            /**
             * @phpstan-var J $result
             * @phpstan-ignore argument.type
             */
            $result = $target->handleInput($input, $fieldsOperator, $fieldsIgnoreCase);

            return $result;
        }

        throw new RuntimeException(sprintf('Unsupported request type: %s', $request::class));
    }

    #[Override]
    public function isCursored(): bool
    {
        return $this->cursor !== null;
    }

    /** @phpstan-return CursorPaginatorInterface<T>|null */
    #[Override]
    public function cursorPaginator(): CursorPaginatorInterface|null
    {
        $this->assertNoSpecifications();

        if ($this->cursorPaginatorInstance !== null) {
            return $this->cursorPaginatorInstance;
        }

        if ($this->cursor === null) {
            return null;
        }

        if (! $this->queryStrategy->supportsCursorPagination()) {
            throw new LogicException('Cursor pagination is not supported by the active Elasticsearch query strategy.');
        }

        if ($this->rawQuerySearchPayload !== null) {
            // Raw query mode lets the caller send a hand-crafted ES body; mixing it with
            // cursor mode would require us to reach into that opaque payload and rewrite
            // its sort/search_after, which defeats the purpose of "raw".
            throw new LogicException('Cursor pagination cannot be combined with raw query search.');
        }

        [$token, $limit] = $this->cursor;
        $cursor          = $token !== null ? $this->cursorCodec->decode($token) : null;

        $effectiveSort = $this->buildCursorEffectiveSort();
        $signature     = Cursor::signatureFor($effectiveSort);
        if ($cursor !== null && $cursor->getSortSignature() !== $signature) {
            throw new InvalidCursorException('Cursor was issued under a different sort order.');
        }

        $direction   = $cursor?->getDirection() ?? Direction::FORWARD;
        $orderBySort = $direction === Direction::BACKWARD ? $effectiveSort->invert() : $effectiveSort;

        $searchAfter = $cursor !== null ? $this->cursorSearchAfter($cursor, $effectiveSort) : null;

        $params   = $this->getCursorParams($orderBySort, $searchAfter, $limit + 1);
        $response = $this->client->search($params, $this->index);
        $result   = $response->getResult();

        $hitsList = $result['hits']['hits'] ?? null;
        if (! is_array($hitsList)) {
            throw new RuntimeException('Unexpected Elasticsearch response: missing hits.hits array.');
        }

        $itemNormalizer = $this->itemNormalizer;
        $hasNormalizer  = is_callable($itemNormalizer);

        $rows       = [];
        $sortValues = [];
        foreach ($hitsList as $hit) {
            /** @phpstan-var array<string, mixed> $hit */
            /** @phpstan-var mixed $source */
            $source = $hit['_source'] ?? [];
            $row    = $hasNormalizer && $itemNormalizer !== null ? $itemNormalizer($source) : $source;
            /** @phpstan-var T $row */
            $rows[] = $row;
            /** @phpstan-var mixed $sortField */
            $sortField    = $hit['sort'] ?? null;
            $sortValues[] = is_array($sortField) ? array_values($sortField) : null;
        }

        /** @phpstan-var ElasticSearchCursorPaginator<T> $paginator */
        $paginator = new ElasticSearchCursorPaginator(
            $rows,
            $sortValues,
            $effectiveSort,
            $direction,
            $limit,
            $this->cursorCodec,
            $cursor !== null,
        );

        $this->cursorPaginatorInstance = $paginator;

        return $paginator;
    }

    /**
     * Compose the SortExpression that anchors cursor pagination: the last non-empty
     * sort from the active query expressions wins; a root-identifier ASC tiebreaker is
     * appended if absent so the order is total (no ambiguous keysets between rows that
     * share the user-specified sort values).
     */
    private function buildCursorEffectiveSort(): SortExpression
    {
        $sort = SortExpression::create();
        foreach ($this->queryExpressions as $qe) {
            $qeSort = $qe->getSort();
            if ($qeSort === null || $qeSort->isSortEmpty()) {
                continue;
            }

            $sort = $qeSort;
        }

        $queryExpressionProvider = $this->getOrCreateQueryExpressionProvider();
        $rootIdentifier          = $queryExpressionProvider->requireSingleRootIdentifier();

        if ($sort->dir($rootIdentifier) === null) {
            $sort = $sort->asc($rootIdentifier);
        }

        return $sort;
    }

    /**
     * Translate the cursor's position (a list of field/value pairs in effective-sort
     * order) into the positional array that ES expects in `search_after`.
     *
     * @phpstan-return list<scalar|null>
     */
    private function cursorSearchAfter(Cursor $cursor, SortExpression $effectiveSort): array
    {
        $sortItems = array_values($effectiveSort->items());
        $position  = $cursor->getPosition();

        if (count($position) !== count($sortItems)) {
            throw new InvalidCursorException('Cursor position does not match the effective sort.');
        }

        $values = [];
        foreach ($sortItems as $i => $sortItem) {
            if ($position[$i]['field'] !== $sortItem['field']) {
                throw new InvalidCursorException('Cursor position does not match the effective sort.');
            }

            $values[] = $position[$i]['value'];
        }

        return $values;
    }

    /**
     * @phpstan-param list<scalar|null>|null $searchAfter
     * @phpstan-param int<1, max>            $size
     *
     * @phpstan-return array<string, mixed>
     */
    private function getCursorParams(SortExpression $orderBySort, array|null $searchAfter, int $size): array
    {
        $queryExpression = $this->getWrappedQueryExpression() ?? QueryExpression::create();
        $provider        = $this->getOrCreateQueryExpressionProvider();
        if (! ($provider instanceof QueryExpressionProvider)) {
            throw new LogicException(sprintf(
                'Cursor pagination requires a QueryExpressionProvider that supports applyCursor(); got %s.',
                $provider::class,
            ));
        }

        return $provider->applyCursor(
            $queryExpression,
            $orderBySort,
            $searchAfter,
            $size,
            null,
            [
                'getIndexMappingFn'  => fn () => $this->client->getFlattenedMapping($this->index, $this->queryStrategy->extractMappingProperties(...)),
                'fullTextSearchTerm' => $this->fullTextSearchTerm,
            ],
        );
    }
}
