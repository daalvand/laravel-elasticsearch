<?php

namespace Daalvand\LaravelElasticsearch\Console\Mappings\Traits;

use Exception;

/**
 * Trait GetsIndices
 * @package Daalvand\LaravelElasticsearch\Console\Mappings\Traits
 */
trait GetsIndices
{
    /**
     * @return array
     */
    protected function indices(): array
    {
        try {
            return collect($this->client->cat()->indices())->sortBy('index')->toArray();
        } catch (Exception $exception) {
            $this->error('Failed to retrieve indices.');
        }

        return [];
    }
}
