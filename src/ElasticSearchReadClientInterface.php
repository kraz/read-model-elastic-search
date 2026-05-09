<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

use Kraz\ReadModel\ReadResponse;

/** @phpstan-template-covariant T of object|array<string, mixed> */
interface ElasticSearchReadClientInterface
{
    /**
     * @phpstan-param array<string, mixed> $query
     *
     * @phpstan-return ReadResponse<covariant T>
     */
    public function read(array $query = [], ?string $index = null): ReadResponse;
}
