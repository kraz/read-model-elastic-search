<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests\Fixtures\Specifications;

use Kraz\ReadModel\Query\FilterExpression;
use Kraz\ReadModel\Query\QueryExpression;
use Kraz\ReadModel\Specification\AbstractSpecification;

use function is_array;

/** @phpstan-extends AbstractSpecification<array<string, mixed>> */
final class StatusEqualsSpecification extends AbstractSpecification
{
    public function __construct(private readonly string $status)
    {
    }

    /** @phpstan-param array<string, mixed> $item */
    public function isSatisfiedBy(object|array $item): bool
    {
        $satisfies = is_array($item) && ($item['status'] ?? null) === $this->status;

        return $this->inverted() ? ! $satisfies : $satisfies;
    }

    protected function buildQueryExpression(): QueryExpression
    {
        return QueryExpression::create()->andWhere(
            FilterExpression::create()->equalTo('status', $this->status),
        );
    }
}
