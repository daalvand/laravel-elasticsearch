<?php

namespace Daalvand\LaravelElasticsearch;

use Daalvand\LaravelElasticsearch\Console\Mappings\IndexAliasCommand;
use Daalvand\LaravelElasticsearch\Console\Mappings\IndexCopyCommand;
use Daalvand\LaravelElasticsearch\Console\Mappings\IndexListCommand;
use Daalvand\LaravelElasticsearch\Console\Mappings\IndexRemoveCommand;
use Daalvand\LaravelElasticsearch\Console\Mappings\IndexSwapCommand;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;

/**
 * Class ElasticsearchServiceProvider
 *
 * @package Daalvand\LaravelElasticsearch
 */
class ElasticsearchServiceProvider extends ServiceProvider
{

    private const COMMANDS = [
        IndexAliasCommand::class,
        IndexCopyCommand::class,
        IndexListCommand::class,
        IndexRemoveCommand::class,
        IndexSwapCommand::class,
    ];

    /**
     * Boot the service provider.
     *
     * @return void
     */
    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands(self::COMMANDS);
        }
        $this->publishes([
            __DIR__ . '/Config/laravel-elasticsearch.php' => config_path('laravel-elasticsearch.php')
        ]);
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register():void
    {
        $config = __DIR__ . '/Config/connection.php';
        $this->mergeConfigFrom($config, 'database.connections.elasticsearch');
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('elasticsearch', function ($config, $name) {
                $config['name'] = $name;
                return new Connection($config);
            });
        });

    }
}
