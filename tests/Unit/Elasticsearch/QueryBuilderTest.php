<?php

namespace Daalvand\LaravelElasticsearch\Tests\Unit\Elasticsearch;

use Daalvand\LaravelElasticsearch\Connection;
use Daalvand\LaravelElasticsearch\Builder;
use Daalvand\LaravelElasticsearch\QueryGrammar;
use Daalvand\LaravelElasticsearch\QueryProcessor;
use Mockery;
use Tests\TestCase;

class QueryBuilderTest extends TestCase
{
    /** @var Builder */
    private $builder;

    public function setUp():void
    {
        parent::setUp();

        /** @var Connection|Mockery\MockInterface $connection */
        $connection = Mockery::mock(Connection::class);

        /** @var QueryGrammar|Mockery\MockInterface $queryGrammar */
        $queryGrammar = Mockery::mock( QueryGrammar::class );

        /** @var QueryProcessor|Mockery\MockInterface $queryProcessor */
        $queryProcessor = Mockery::mock( QueryProcessor::class );

        $this->builder = new Builder($connection, $queryGrammar, $queryProcessor);
    }

    /**
     * @test
     * @dataProvider whereParentIdProvider
     * @param string $parentType
     * @param        $id
     * @param string $boolean
     */
    public function adds_parent_id_to_wheres_clause(string $parentType, $id, string $boolean):void
    {
        $this->builder->whereParentId($parentType, $id, $boolean);

        self::assertEquals([
            'type' => 'ParentId',
            'name' => $parentType,
            'id' => $id,
            'boolean' => $boolean,
        ], $this->builder->wheres[0]);
    }

    /**
     * @return array
     */
    public function whereParentIdProvider():array
    {
        return [
            'boolean and' => ['my_parent', 1, 'and'],
            'boolean or' => ['my_parent', 1, 'or'],
        ];
    }
}
