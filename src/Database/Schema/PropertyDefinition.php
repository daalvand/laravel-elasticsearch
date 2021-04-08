<?php

namespace Daalvand\LaravelElasticsearch\Database\Schema;

use Closure;
use Illuminate\Support\Fluent;

/**
 * Class PropertyDefinition
 * @method self boost(int $boost)
 * @method self dynamic(bool $dynamic = true)
 * @method self fields(Closure $field)
 * @method self format(string $format)
 * @method self index(bool $index = true)
 * @method self properties(Closure $field)
 * @package Daalvand\LaravelElasticsearch\Database\Schema
 */
class PropertyDefinition extends Fluent
{
    //
}
