<?php

namespace Daalvand\LaravelElasticsearch;

use Illuminate\Database\Eloquent\Collection as BaseCollection;

class Collection extends BaseCollection
{
    public function addToIndex()
    {
        if ($this->isEmpty()) {
            return null;
        }

        $instance = $this->first();
        $instance->setConnection($instance->getElasticsearchConnectionName());
        $query = $this->first()->newQueryWithoutScopes();

        $docs = $this->map(function ($model, $i) {
            return $model->toSearchableArray();
        });

        $success = $query->insert($docs->all());

        unset($docs);

        return $success;
    }
}
