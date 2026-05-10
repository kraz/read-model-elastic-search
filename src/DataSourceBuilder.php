<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use InvalidArgumentException;
use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ReadModel\Query\QueryExpressionProviderInterface;
use Kraz\ReadModel\ReadDataProviderBuilder;
use Kraz\ReadModel\ReadDataProviderBuilderInterface;
use Kraz\ReadModel\ReadDataProviderCompositionInterface;
use Kraz\ReadModel\ReadModelDescriptorFactoryInterface;
use Kraz\ReadModelElasticSearch\Query\QueryExpressionProvider;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategy9x;
use Kraz\ReadModelElasticSearch\QueryStrategy\QueryStrategyInterface;
use LogicException;
use Override;

/**
 * @phpstan-template-covariant T of object|array<string, mixed> = array<string, mixed>
 * @implements ReadDataProviderCompositionInterface<T>
 * @implements ReadDataProviderBuilderInterface<T>
 * @implements FullTextSearchReadModelInterface<T>
 * @implements ElasticRawQuerySearchReadModelInterface<T>
 */
class DataSourceBuilder implements ReadDataProviderCompositionInterface, ReadDataProviderBuilderInterface, FullTextSearchReadModelInterface, ElasticRawQuerySearchReadModelInterface
{
    /** @use ReadDataProviderBuilder<T> */
    use ReadDataProviderBuilder;

    private mixed $data                                = null;
    private string|null $fullTextSearchTerm            = null;
    private string|null $rawQuerySearchPayload         = null;
    private QueryStrategyInterface|null $queryStrategy = null;

    protected function createDefaultQueryExpressionProvider(ReadModelDescriptorFactoryInterface $factory): QueryExpressionProviderInterface
    {
        $this->queryStrategy ??= new QueryStrategy9x();

        return new QueryExpressionProvider($factory, $this->queryStrategy);
    }

    /**
     * @phpstan-param ElasticSearchClientInterface&ElasticSearchReadClientInterface<J> $data
     *
     * @phpstan-return static<J>
     *
     * @phpstan-template J of object|array<string, mixed> = array<string, mixed>
     */
    public function withData(ElasticSearchClientInterface&ElasticSearchReadClientInterface $data): static
    {
        /** @phpstan-var static<J> $clone */
        $clone       = clone $this;
        $clone->data = $data;

        return $clone;
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
        $clone                        = clone $this;
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

    /** @phpstan-return static<T> */
    public function withQueryStrategy(QueryStrategyInterface $queryStrategy): static
    {
        /** @phpstan-var static<T> $clone */
        $clone                = clone $this;
        $clone->queryStrategy = $queryStrategy;

        return $clone;
    }

    /**
     * @phpstan-param (ElasticSearchClientInterface&ElasticSearchReadClientInterface<J>)|null $data
     *
     * @return ($data is null ? DataSource<object|array<string, mixed>> : DataSource<J>)
     *
     * @phpstan-template J of object|array<string, mixed> = array<string, mixed>
     */
    public function create(mixed $data = null, string|null $index = null): DataSource
    {
        $data ??= $this->data;
        if ($data === null) {
            throw new InvalidArgumentException('The data source has no data assigned! Expected a value other than null.');
        }

        if (! $data instanceof ElasticSearchClientInterface || ! $data instanceof ElasticSearchReadClientInterface) {
            throw new InvalidArgumentException('Unsupported datasource data!');
        }

        $args = [
            'client' => $data,
            'index' => $index,
            'queryExpressionProvider' => $this->queryExpressionProvider,
        ];
        if ($this->queryStrategy !== null) {
            $args['queryStrategy'] = $this->queryStrategy;
        }

        /** @phpstan-var DataSource<J> $dataSource */
        $dataSource = new DataSource(...$args);

        if ($this->fullTextSearchTerm !== null) {
            $dataSource = $dataSource->withFullTextSearch($this->fullTextSearchTerm);
        }

        if ($this->rawQuerySearchPayload !== null) {
            $dataSource = $dataSource->withRawQuerySearch($this->rawQuerySearchPayload);
        }

        return $this->apply($dataSource);
    }

    #[Override]
    public function handleRequest(object $request, array $fieldsOperator = [], array $fieldsIgnoreCase = []): static
    {
        throw new LogicException('Unsupported operation. The data source builder can not handle requests.');
    }
}
