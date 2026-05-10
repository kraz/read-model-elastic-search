<?php

declare(strict_types=1);

namespace Kraz\ReadModelElasticSearch\Tests;

use InvalidArgumentException;
use Kraz\ElasticSearchClient\ElasticSearchClientInterface;
use Kraz\ElasticSearchClient\ElasticSearchResponse;
use Kraz\ReadModelElasticSearch\ElasticSearchClientGateway;
use Kraz\ReadModelElasticSearch\ElasticSearchDenormalizerInterface;
use Nyholm\Psr7\Factory\Psr17Factory;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

/**
 * Concrete subclass that exposes the protected handleRead() method for testing.
 */
final class ConcreteElasticSearchClientGateway extends ElasticSearchClientGateway
{
    /**
     * @phpstan-param array<string, mixed>|null $query
     * @phpstan-param class-string<object>|null $responseClass
     *
     * @phpstan-return object|array<string, mixed>
     */
    public function exposedHandleRead(
        array|null $query = null,
        string|null $responseClass = null,
        string|null $index = null,
    ): object|array {
        return $this->handleRead($query, $responseClass, $index);
    }
}

#[CoversClass(ElasticSearchClientGateway::class)]
final class ElasticSearchClientGatewayTest extends TestCase
{
    /** @param array<string, mixed> $data */
    private function makeEsResponse(array $data, int $status = 200): ElasticSearchResponse
    {
        $factory  = new Psr17Factory();
        $body     = $factory->createStream((string) json_encode($data));
        $response = $factory->createResponse($status)->withBody($body);

        return new ElasticSearchResponse($response);
    }

    /** @param array<string, mixed>[] $sources */
    private function makeHitsResponse(array $sources, int $total): ElasticSearchResponse
    {
        $hits = array_map(static fn (array $src) => ['_source' => $src], $sources);

        return $this->makeEsResponse([
            'hits' => [
                'total' => ['value' => $total],
                'hits'  => $hits,
            ],
        ]);
    }

    private function makeGateway(
        ElasticSearchClientInterface $api,
        ElasticSearchDenormalizerInterface $denormalizer,
    ): ConcreteElasticSearchClientGateway {
        return new ConcreteElasticSearchClientGateway($api, $denormalizer);
    }

    // -------------------------------------------------------------------------
    // handleRead – payload extraction
    // -------------------------------------------------------------------------

    public function testHandleReadExtractsSourcesFromHits(): void
    {
        $sources = [
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ];

        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse($sources, 2));

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 10]);

        self::assertIsArray($result);
        self::assertSame($sources, $result['data']);
        self::assertSame(2, $result['total']);
    }

    public function testHandleReadCalculatesPageFromFromAndSize(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([], 100));

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->exposedHandleRead(['query' => [], 'from' => 20, 'size' => 10]);

        self::assertIsArray($result);
        self::assertSame(3, $result['page']);
    }

    public function testHandleReadPageIsOneWhenFromIsZero(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([], 5));

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 10]);

        self::assertIsArray($result);
        self::assertSame(1, $result['page']);
    }

    public function testHandleReadPageIsOneWhenSizeIsZero(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([], 5));

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 0]);

        self::assertIsArray($result);
        self::assertSame(1, $result['page']);
    }

    public function testHandleReadUsesDefaultQueryWhenNullIsPassed(): void
    {
        $capturedQuery = null;

        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturnCallback(
            function (array $query) use (&$capturedQuery): ElasticSearchResponse {
                $capturedQuery = $query;

                return $this->makeHitsResponse([], 0);
            },
        );

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $gateway->exposedHandleRead(null);

        self::assertIsArray($capturedQuery);
        self::assertArrayHasKey('query', $capturedQuery);
        self::assertSame(0, $capturedQuery['from']);
        self::assertSame(10, $capturedQuery['size']);
    }

    // -------------------------------------------------------------------------
    // handleRead – denormalizer
    // -------------------------------------------------------------------------

    public function testHandleReadCallsDenormalizerWhenResponseClassNameProvided(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([['id' => 1]], 1));

        $responseObject = new \stdClass();

        $denormalizer = $this->createMock(ElasticSearchDenormalizerInterface::class);
        $denormalizer->expects(self::once())
            ->method('denormalize')
            ->with(
                self::callback(static fn (array $p): bool => $p['total'] === 1 && $p['page'] === 1),
                \stdClass::class,
            )
            ->willReturn($responseObject);

        $gateway = $this->makeGateway($api, $denormalizer);
        $result  = $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 10], \stdClass::class);

        self::assertSame($responseObject, $result);
    }

    public function testHandleReadReturnsArrayWhenNoResponseClassNameProvided(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([['id' => 1]], 1));

        $denormalizer = $this->createMock(ElasticSearchDenormalizerInterface::class);
        $denormalizer->expects(self::never())->method('denormalize');

        $gateway = $this->makeGateway($api, $denormalizer);
        $result  = $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 10]);

        self::assertIsArray($result);
    }

    public function testHandleReadThrowsWhenDenormalizerReturnsWrongType(): void
    {
        $api = $this->createStub(ElasticSearchClientInterface::class);
        $api->method('search')->willReturn($this->makeHitsResponse([], 0));

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $denormalizer->method('denormalize')->willReturn(new \stdClass());

        $gateway = $this->makeGateway($api, $denormalizer);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected denormalized response to be an instance of');

        // Pass a class name that is NOT stdClass to trigger the type mismatch
        $gateway->exposedHandleRead(['query' => [], 'from' => 0, 'size' => 10], \ArrayObject::class);
    }

    // -------------------------------------------------------------------------
    // Delegation methods
    // -------------------------------------------------------------------------

    public function testSearchDelegatesToApi(): void
    {
        $expected = $this->makeHitsResponse([], 0);

        $api = $this->createMock(ElasticSearchClientInterface::class);
        $api->expects(self::once())
            ->method('search')
            ->with(['query' => []], 'my_index')
            ->willReturn($expected);

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->search(['query' => []], 'my_index');

        self::assertSame($expected, $result);
    }

    public function testGetMappingDelegatesToApi(): void
    {
        $expected = $this->makeEsResponse([]);

        $api = $this->createMock(ElasticSearchClientInterface::class);
        $api->expects(self::once())
            ->method('getMapping')
            ->with('my_index')
            ->willReturn($expected);

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->getMapping('my_index');

        self::assertSame($expected, $result);
    }

    public function testGetFlattenedMappingDelegatesToApi(): void
    {
        $expected = ['name' => ['type' => 'text', 'nestedPath' => null]];

        $api = $this->createMock(ElasticSearchClientInterface::class);
        $api->expects(self::once())
            ->method('getFlattenedMapping')
            ->willReturn($expected);

        $denormalizer = $this->createStub(ElasticSearchDenormalizerInterface::class);
        $gateway      = $this->makeGateway($api, $denormalizer);

        $result = $gateway->getFlattenedMapping('my_index');

        self::assertSame($expected, $result);
    }
}
