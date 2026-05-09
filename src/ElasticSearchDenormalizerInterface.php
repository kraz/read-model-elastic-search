<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch;

interface ElasticSearchDenormalizerInterface
{
    /**
     * @phpstan-param class-string<T>|string $type
     *
     * @phpstan-return ($type is class-string<T> ? T : mixed)
     *
     * @phpstan-template T of object
     */
    public function denormalize(mixed $data, string $type): mixed;
}
