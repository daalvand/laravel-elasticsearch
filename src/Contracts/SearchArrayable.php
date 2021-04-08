<?php

namespace Daalvand\LaravelElasticsearch\Contracts;

/**
 * Interface SearchArrayable
 *
 * @package Daalvand\LaravelElasticsearch\Contracts
 */
interface SearchArrayable
{
    /**
     * @return array
     */
    public function toSearchableArray():array;
}
