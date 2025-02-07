<?php

namespace Daalvand\LaravelElasticsearch\Tests\Unit\Database\Schema\Grammars;

use Carbon\Carbon;
use Daalvand\LaravelElasticsearch\Connection;
use Daalvand\LaravelElasticsearch\Database\Schema\Blueprint;
use Daalvand\LaravelElasticsearch\Database\Schema\Grammars\ElasticsearchGrammar;
use Elasticsearch\Client;
use Elasticsearch\Namespaces\CatNamespace;
use Elasticsearch\Namespaces\IndicesNamespace;
use Illuminate\Support\Fluent;
use Mockery;
use Tests\TestCase;

class ElasticsearchGrammarTest extends TestCase
{
    /** @var Blueprint|Mockery\CompositeExpectation */
    private $blueprint;

    /** @var Connection|Mockery\CompositeExpectation */
    private $connection;

    /** @var ElasticsearchGrammar */
    private $grammar;

    public function setUp():void
    {
        parent::setUp();

        /** @var CatNamespace|Mockery\CompositeExpectation $catNamespace */
        $catNamespace = Mockery::mock(CatNamespace::class);
        $catNamespace->shouldReceive('indices')->andReturn([]);

        /** @var IndicesNamespace|Mockery\CompositeExpectation $indicesNamespace */
        $indicesNamespace = Mockery::mock(IndicesNamespace::class);
        $indicesNamespace->shouldReceive('existsAlias')->andReturnFalse();

        /** @var Client|Mockery\CompositeExpectation $client */
        $client = Mockery::mock(Client::class);
        $client->shouldReceive('cat')->andReturn($catNamespace);
        $client->shouldReceive('indices')->andReturn($indicesNamespace);

        /** @var Connection|Mockery\CompositeExpectation $connection */
        $connection = Mockery::mock(Connection::class)->makePartial()->shouldAllowMockingProtectedMethods();
        $connection->shouldReceive('createConnection')->andReturn($client);
        Carbon::setTestNow(Carbon::create(2019, 7, 2, 12));
        $this->blueprint = new Blueprint('indices');
        $this->connection = $connection;
        $this->grammar = new ElasticsearchGrammar();
    }

    /**
     * It returns a closure that will create an index.
     * @test
     * @covers ElasticsearchGrammar::compileCreate
     */
    public function it_returns_a_closure_that_will_create_an_index()
    {
        $alias = 'indices_dev';
        $index = '2019_07_02_120000_indices_dev';
        $mapping = [
            'mappings' => [
                'properties' => [
                    'title' => [
                        'type' => 'text',
                        'fields' => [
                            'raw' => [
                                'type' => 'keyword'
                            ]
                        ]
                    ],
                    'date' => [
                        'type' => 'date'
                    ]
                ]
            ]
        ];
        $blueprint = clone($this->blueprint);

        $blueprint->text('title')->fields(function (Blueprint $mapping): void {
            $mapping->keyword('raw');
        });
        $blueprint->date('date');

        /** @var IndicesNamespace|Mockery\CompositeExpectation $indicesNamespace */
        $indicesNamespace = Mockery::mock(IndicesNamespace::class);
        $indicesNamespace->shouldReceive('create')->once()->with(['index' => $index, 'body' => $mapping]);
        $indicesNamespace->shouldReceive('existsAlias')->once()->with(['name' => $alias])->andReturnFalse();
        $indicesNamespace->shouldReceive('putAlias')->once()->with(['index' => $index, 'name' => $alias]);

        $this->connection->shouldReceive('indices')->andReturn($indicesNamespace);
        $this->connection->shouldReceive('createAlias')->once()->with($index, $alias)->passthru();

        $this->connection->shouldReceive('createIndex')->once()->with($index, $mapping)->passthru();
        $this->grammar->compileCreate($blueprint, new Fluent(), $this->connection);
    }

    /**
     * It returns a closure that will drop an index.
     * @test
     * @covers ElasticsearchGrammar::compileDrop
     */
    public function it_returns_a_closure_that_will_drop_an_index()
    {
        $index = '2019_06_03_120000_indices_dev';
        /** @var CatNamespace|Mockery\CompositeExpectation $catNamespace */
        $catNamespace = Mockery::mock(CatNamespace::class);
        $catNamespace->shouldReceive('indices')->andReturn([
            ['index' => $index]
        ]);
        /** @var IndicesNamespace|Mockery\CompositeExpectation $indicesNamespace */
        $indicesNamespace = Mockery::mock(IndicesNamespace::class);
        $indicesNamespace->shouldReceive('delete')->once()->with(['index' => $index]);
        $this->connection->shouldReceive('cat')->andReturn($catNamespace);
        $this->connection->shouldReceive('indices')->andReturn($indicesNamespace);
        $this->connection->shouldReceive('dropIndex')->once()->with($index)->passthru();
        $this->grammar->compileDrop($this->blueprint, new Fluent(), $this->connection);
    }

    /**
     * It returns a closure that will drop an index if it exists.
     * @test
     * @covers       ElasticsearchGrammar::compileDropIfExists
     * @dataProvider compile_drop_if_exists_data_provider
     */
    public function it_returns_a_closure_that_will_drop_an_index_if_it_exists($table, $times)
    {
        $index = '2019_06_03_120000_indices_dev';
        $this->blueprint = new Blueprint($table);

        /** @var CatNamespace|Mockery\CompositeExpectation $catNamespace */
        $catNamespace = Mockery::mock(CatNamespace::class);
        $catNamespace->shouldReceive('indices')->andReturn([
            ['index' => $index]
        ]);

        /** @var IndicesNamespace|Mockery\CompositeExpectation $indicesNamespace */
        $indicesNamespace = Mockery::mock(IndicesNamespace::class);
        $indicesNamespace->shouldReceive('delete')->times($times)->with(['index' => $index]);

        $this->connection->shouldReceive('indices')->andReturn($indicesNamespace);
        $this->connection->shouldReceive('cat')->once()->andReturn($catNamespace);
        $this->connection->shouldReceive('dropIndex')->times($times)->with($index)->passthru();
        $this->grammar->compileDropIfExists($this->blueprint, new Fluent(), $this->connection);
    }

    /**
     * compileDropIfExists data provider.
     */
    public function compile_drop_if_exists_data_provider(): array
    {
        return [
            'it exists' => ['indices', 1],
            'it does not exists' => ['books', 0]
        ];
    }

    /**
     * It returns a closure that will update an index mapping.
     * @test
     * @covers ElasticsearchGrammar::compileUpdate
     */
    public function it_returns_a_closure_that_will_update_an_index_mapping()
    {
        $this->blueprint->text('title');
        $this->blueprint->date('date');
        $this->blueprint->keyword('status');

        $this->connection->shouldReceive('updateIndex')->once()->with('indices_dev', 'index', [
            'index' => [
                'properties' => [
                    'title' => [
                        'type' => 'text'
                    ],
                    'date' => [
                        'type' => 'date'
                    ],
                    'status' => [
                        'type' => 'keyword'
                    ]
                ]
            ]
        ]);

        $this->grammar->compileUpdate($this->blueprint, new Fluent(), $this->connection);
    }

    /**
     * It generates a mapping.
     * @test
     * @covers ElasticsearchGrammar::getColumns
     */
    public function it_generates_a_mapping()
    {
        $this->blueprint->join('joins', ['parent' => 'child']);
        $this->blueprint->text('title')->fields(function (Blueprint $field) {
            $field->keyword('raw');
        });
        $this->blueprint->date('start_date');
        $this->blueprint->boolean('is_closed');
        $this->blueprint->keyword('status');
        $this->blueprint->float('price');
        $this->blueprint->integer('total_reviews');
        $this->blueprint->object('location')->properties(function (Blueprint $mapping) {
            $mapping->text('address');
            $mapping->text('postcode');
            $mapping->geoPoint('coordinates');
        });

        $expected = [
            'joins' => [
                'type' => 'join',
                'relations' => [
                    'parent' => 'child'
                ],
            ],
            'title' => [
                'type' => 'text',
                'fields' => [
                    'raw' => [
                        'type' => 'keyword'
                    ]
                ]
            ],
            'start_date' => [
                'type' => 'date'
            ],
            'is_closed' => [
                'type' => 'boolean'
            ],
            'status' => [
                'type' => 'keyword'
            ],
            'price' => [
                'type' => 'float'
            ],
            'total_reviews' => [
                'type' => 'integer'
            ],
            'location' => [
                'properties' => [
                    'address' => [
                        'type' => 'text',
                    ],
                    'postcode' => [
                        'type' => 'text'
                    ],
                    'coordinates' => [
                        'type' => 'geo_point'
                    ]
                ]
            ]
        ];

        $grammar = new class extends ElasticsearchGrammar
        {
            public function outputMapping(Blueprint $blueprint)
            {
                return $this->getColumns($blueprint);
            }
        };
        self::assertEquals($expected, $grammar->outputMapping($this->blueprint));
    }
}
