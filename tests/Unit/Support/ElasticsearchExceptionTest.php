<?php

namespace Daalvand\LaravelElasticsearch\Tests\Unit\Support;

use Daalvand\LaravelElasticsearch\Support\ElasticsearchException;
use Elasticsearch\Common\Exceptions\ElasticsearchException as BaseElasticsearchException;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Orchestra\Testbench\TestCase;

class ElasticsearchExceptionTest extends TestCase
{
    /**
     * @test
     * @dataProvider errorMessagesProvider
     * @param BaseElasticsearchException $exception
     * @param string                     $code
     */
    public function returns_the_error_code(BaseElasticsearchException $exception, string $code): void
    {
        $exception = new ElasticsearchException($exception);

        self::assertSame($code, $exception->getCode());
    }

    /**
     * @test
     * @dataProvider errorMessagesProvider
     * @param BaseElasticsearchException $exception
     * @param string                     $message
     */
    public function returns_the_error_message(BaseElasticsearchException $exception, string $message): void
    {
        $exception = new ElasticsearchException($exception);

        self::assertSame($message, $exception->getMessage());
    }

    /**
     * @test
     * @dataProvider errorMessagesProvider
     * @param BaseElasticsearchException $exception
     * @param string                     $code
     * @param string                     $message
     */
    public function converts_the_error_to_string(
        BaseElasticsearchException $exception,
        string $code,
        string $message
    ): void
    {
        $exception = new ElasticsearchException($exception);
        self::assertSame("$code: $message", (string)$exception);
    }

    /**
     * @test
     * @dataProvider errorMessagesProvider
     * @param BaseElasticsearchException $exception
     * @param array                      $raw
     */
    public function returns_the_raw_error_message_as_an_array(
        BaseElasticsearchException $exception,
        array $raw
    ): void
    {
        $exception = new ElasticsearchException($exception);

        self::assertSame($raw, $exception->getRaw());
    }

    public function errorMessagesProvider(): array
    {
        $response          = [
            "error"  => [
                "root_cause"    => [
                    [
                        "type"          => "index_not_found_exception",
                        "reason"        => "no such index [bob]",
                        "resource.type" => "index_or_alias",
                        "resource.id"   => "bob",
                        "index_uuid"    => "_na_",
                        "index"         => "bob",
                    ],
                ],
                "type"          => "index_not_found_exception",
                "reason"        => "no such index [bob]",
                "resource.type" => "index_or_alias",
                "resource.id"   => "bob",
                "index_uuid"    => "_na_",
                "index"         => "bob",
            ],
            "status" => 404,
        ];
        $missingIndexError = json_encode($response);

        return [
            'missing_index' => [
                'error'   => new Missing404Exception($missingIndexError),
                'code'    => 'index_not_found_exception',
                'message' => 'no such index [bob]',
                'raw'     => json_decode($missingIndexError, true),
            ],
        ];
    }
}
