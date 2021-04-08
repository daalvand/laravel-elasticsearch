<?php

namespace Daalvand\LaravelElasticsearch\Database\Schema;

use Closure;
use Illuminate\Database\Schema\Builder;

/**
 * Class Builder
 * @package Daalvand\LaravelElasticsearch\Database\Schema
 */
class ElasticsearchBuilder extends Builder
{
    /**
     * @param         $table
     * @param Closure $callback
     */
    public function index($table, Closure $callback)
    {
        $this->table($table, $callback);
    }

    /**
     * @param string  $table
     * @param Closure $callback
     */
    public function table($table, Closure $callback)
    {
        $this->build(tap($this->createBlueprint($table), static function (Blueprint $blueprint) use ($callback) {
            $blueprint->update();

            $callback($blueprint);
        }));
    }

    /**
     * @inheritDoc
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        return new Blueprint($table, $callback);
    }
}
