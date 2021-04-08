<?php

namespace Daalvand\LaravelElasticsearch\Tests;

use Illuminate\Foundation\Application;
use Orchestra\Testbench\TestCase as BaseTestCase;

class TestCase extends BaseTestCase
{
    /**
     * @param Application $app
     */
    protected function getEnvironmentSetUp($app):void
    {
        /** @noinspection StaticInvocationViaThisInspection */
        $app['config']->set('database.connections.elasticsearch.suffix', '_test');
    }
}
