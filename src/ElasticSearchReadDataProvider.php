<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use Kraz\ReadModel\DataSourceReadDataProvider;
use Override;

/** @phpstan-template-covariant T of object|array<string, mixed> */
trait ElasticSearchReadDataProvider
{
    /** @use DataSourceReadDataProvider<T> */
    use DataSourceReadDataProvider;

    /** @phpstan-return DataSource<T> */
    abstract protected function createDataSource(): DataSource;

    #[Override]
    public function withFullTextSearch(string $term): static
    {
        $clone = clone $this;
        /** @phpstan-var DataSource<T> $ds */
        $ds                = $clone->dataSource();
        $clone->dataSource = $ds->withFullTextSearch($term);

        return $clone;
    }

    #[Override]
    public function withoutFullTextSearch(): static
    {
        $clone = clone $this;
        /** @phpstan-var DataSource<T> $ds */
        $ds                = $clone->dataSource();
        $clone->dataSource = $ds->withoutFullTextSearch();

        return $clone;
    }

    #[Override]
    public function withRawQuerySearch(string $query): static
    {
        $clone = clone $this;
        /** @phpstan-var DataSource<T> $ds */
        $ds                = $clone->dataSource();
        $clone->dataSource = $ds->withRawQuerySearch($query);

        return $clone;
    }

    #[Override]
    public function withoutRawQuerySearch(): static
    {
        $clone = clone $this;
        /** @phpstan-var DataSource<T> $ds */
        $ds                = $clone->dataSource();
        $clone->dataSource = $ds->withoutRawQuerySearch();

        return $clone;
    }
}
