<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

/**
 * @phpstan-template-covariant T of object|array<string, mixed>
 */
interface RawQuerySearchReadModelInterface
{
    /**
     * @phpstan-return static<T>
     */
    public function withRawQuerySearch(string $query): static;

    /**
     * @phpstan-return static<T>
     */
    public function withoutRawQuerySearch(): static;
}
