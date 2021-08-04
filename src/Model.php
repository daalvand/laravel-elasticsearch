<?php


namespace Daalvand\LaravelElasticsearch;

use Daalvand\LaravelElasticsearch\Traits\HasRelationships;
use Illuminate\Database\Eloquent\Concerns\HasRelationships as BaseHasRelationships;
use Illuminate\Database\Eloquent\Model as BaseModel;

abstract class Model extends BaseModel
{
    use Searchable, HasRelationships, BaseHasRelationships {
        BaseHasRelationships::newBelongsTo as deprecatedNewBelongsTo;
        HasRelationships::newBelongsTo insteadof BaseHasRelationships;
    }
}