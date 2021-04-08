<?php


namespace Daalvand\LaravelElasticsearch\Database\Migrations;

use Illuminate\Database\Migrations\Migration as BaseMigration;
use Illuminate\Database\Schema\Builder;
use Illuminate\Support\Facades\Schema;

class Migration extends BaseMigration
{
    protected Builder $schema;
    public function __construct()
    {
        $this->schema = Schema::connection('elasticsearch');
    }
}
