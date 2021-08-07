<?php

namespace Daalvand\LaravelElasticsearch;

use Illuminate\Support\Collection as SupportCollection;

class AggregationResult
{

    protected SupportCollection $aggregations;
    protected int $total;

    public function __construct(array $aggregations, int $total)
    {
        $this->aggregations = collect($aggregations);
        $this->total        = $total;
    }

    public static function make(array $aggregations, int $total):self
    {
        return new static($aggregations, $total);
    }

    /**
     * @return \Illuminate\Support\Collection
     */
    public function getAggregations(): SupportCollection
    {
        return $this->aggregations;
    }

    /**
     * @param string $aggregation
     * @return array
     */
    public function getAggregation(string $aggregation): array
    {
        return $this->aggregations[$aggregation] ?? [];
    }

    /**
     * get specific aggregation
     * @param $name
     * @return array
     */
    public function __get($name)
    {
        return $this->getAggregation($name);
    }

    /**
     * @return int
     */
    public function getTotal(): int
    {
        return $this->total;
    }


}