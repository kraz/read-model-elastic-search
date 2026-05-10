<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Fixtures\Specifications;

use Kraz\ReadModel\Specification\AbstractSpecification;

use function is_array;

/** @phpstan-extends AbstractSpecification<array<string, mixed>> */
final class OrderValueAboveSpecification extends AbstractSpecification
{
    public function __construct(private readonly float $minValue)
    {
    }

    /** @phpstan-param array<string, mixed> $item */
    public function isSatisfiedBy(object|array $item): bool
    {
        $satisfies = is_array($item) && (float) ($item['orderValue'] ?? 0) > $this->minValue;

        return $this->inverted() ? ! $satisfies : $satisfies;
    }
}
