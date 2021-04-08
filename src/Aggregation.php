<?php

namespace Daalvand\LaravelElasticsearch;

abstract class Aggregation
{
    protected $arguments = [];
    protected $key = '';
    protected $type = '';

    public function __invoke(Builder $query): Builder
    {
        return $query;
    }

    public function getArguments()
    {
        return $this->arguments;
    }

    public function getKey(): string
    {
        return $this->key;
    }

    public function getType(): string
    {
        return $this->type;
    }
}
