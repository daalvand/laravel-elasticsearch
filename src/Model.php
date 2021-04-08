<?php


namespace Daalvand\LaravelElasticsearch;
use Illuminate\Database\Eloquent\Model as BaseModel;

abstract class Model extends BaseModel
{
    use Searchable;
}