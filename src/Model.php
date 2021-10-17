<?php


namespace Daalvand\LaravelElasticsearch;

use Daalvand\LaravelElasticsearch\Traits\HasRelationships;
use Illuminate\Database\Eloquent\Concerns\HasRelationships as BaseHasRelationships;
use Illuminate\Database\Eloquent\Model as BaseModel;
use Illuminate\Support\Str;

abstract class Model extends BaseModel
{
    use Searchable, HasRelationships, BaseHasRelationships {
        BaseHasRelationships::newBelongsTo as deprecatedNewBelongsTo;
        HasRelationships::newBelongsTo insteadof BaseHasRelationships;
    }

    public function qualifyColumn($column)
    {
        return $column;
    }


    public function getTable(): string
    {
        $index = $this->getConnection()->getConfig('index');
        return $this->table ?? $index ?? Str::snake(Str::pluralStudly(class_basename($this)));
    }
}
