<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

/**
 * @phpstan-template-covariant T of object|array<string, mixed>
 */
interface FullTextSearchReadModelInterface
{
    /**
     * @phpstan-return static<T>
     */
    public function withFullTextSearch(string $term): static;

    /**
     * @phpstan-return static<T>
     */
    public function withoutFullTextSearch(): static;
}
