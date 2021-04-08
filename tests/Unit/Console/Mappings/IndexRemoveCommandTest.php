<?php

namespace Daalvand\LaravelElasticsearch\Tests\Unit\Console\Mappings;

use Daalvand\LaravelElasticsearch\Console\Mappings\IndexRemoveCommand;
use Elasticsearch\Client;
use Elasticsearch\Namespaces\CatNamespace;
use Elasticsearch\Namespaces\IndicesNamespace;
use Mockery;
use Orchestra\Testbench\TestCase;

/**
 * Class IndexRemoveCommandTest
 *
 * @package Tests\Console\Mappings
 */
class IndexRemoveCommandTest extends TestCase
{

    /** @var Mockery\CompositeExpectation|IndexRemoveCommand */
    private $command;

    public function setUp():void
    {
        parent::setUp();

        $this->command = Mockery::mock(IndexRemoveCommand::class)
            ->shouldAllowMockingProtectedMethods()
            ->makePartial();
    }

    /**
     * It removes the given index.
     *
     * @test
     * @covers       IndexRemoveCommand::removeIndex()
     */
    public function it_removes_the_given_index()
    {
        $indicesNamespace = Mockery::mock(IndicesNamespace::class);
        $indicesNamespace->shouldReceive('delete')->once()->with(['index' => 'test_index']);

        $client = Mockery::mock(Client::class);
        $client->shouldReceive('indices')->andReturn($indicesNamespace);

        $this->command->client = $client;

        $this->command->shouldReceive('info');

        self::assertTrue($this->command->removeIndex('test_index'));
    }

    /**
     * It handles the console command call.
     *
     * @test
     * @covers IndexRemoveCommand::handle()
     * @dataProvider handle_data_provider
     */
    public function it_handles_the_console_command_call($index)
    {
        $catNamespace = Mockery::mock(CatNamespace::class);
        $catNamespace->shouldReceive('indices')->andReturn([['index' => $index]]);

        $client = Mockery::mock(Client::class);
        $client->shouldReceive('cat')->andReturn($catNamespace);

        $this->command->client = $client;

        $this->command->shouldReceive('argument')->once()->with('index')->andReturn($index);
        $this->command->shouldReceive('choice')->with('Which index would you like to delete?', [$index]);
        $this->command->shouldReceive('confirm')->withAnyArgs()->andReturn((bool)$index);
        $this->command->shouldReceive('removeIndex')->with($index);

        $this->command->handle();
    }

    /**
     * @return array
     */
    public function handle_data_provider():array
    {
        return [
            'index given'    => ['test_index'],
            'no index given' => [null],
        ];
    }
}
