<?php

namespace Daalvand\LaravelElasticsearch\Console\Mappings;

use Daalvand\LaravelElasticsearch\Connection;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Illuminate\Console\Command as BaseCommand;
use Illuminate\Support\Facades\Config;

/**
 * Class Command
 * @package Daalvand\LaravelElasticsearch\Console\Mappings
 */
abstract class Command extends BaseCommand
{

    /** @var ClientBuilder|Client $client */
    public $client;

    /**
     * Command constructor.
     */
    public function __construct()
    {
        parent::__construct();

        $this->client = new Connection(Config::get('database.connections.elasticsearch'));
    }

    /**
     * @return void
     */
    abstract public function handle();
}
