<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

/**
 * @phpstan-template-covariant T of object|array<string, mixed>
 * @extends RawQuerySearchReadModelInterface<T>
 */
interface ElasticRawQuerySearchReadModelInterface extends RawQuerySearchReadModelInterface
{
}
