<?php

namespace Daalvand\LaravelElasticsearch\Database\Schema;

use Carbon\Carbon;
use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;
use Illuminate\Database\Schema\ColumnDefinition;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Fluent;
use Illuminate\Support\Str;

/**
 * Class Blueprint
 * @package Daalvand\LaravelElasticsearch\Database\Schema
 */
class Blueprint extends BaseBlueprint
{
    protected string $alias;
    protected string $document;
    protected array $meta = [];
    protected array $indexSettings = [];

    /**
     * @inheritDoc
     */
    public function addColumn($type, $name, array $parameters = [])
    {
        $attributes = ['name'];

        if (isset($type)) {
            $attributes[] = 'type';
        }

        $this->columns[] = $column = new PropertyDefinition(
            array_merge(compact(...$attributes), $parameters)
        );

        return $column;
    }

    /**
     * @param string $key
     * @param array  $value
     */
    public function addIndexSettings(string $key, array $value): void
    {
        $this->indexSettings[$key] = $value;
    }

    /**
     * @param string $key
     * @param        $value
     */
    public function addMetaField(string $key, $value): void
    {
        $this->meta[$key] = $value;
    }

    /**
     * @param string $alias
     */
    public function alias(string $alias): void
    {
        $this->alias = $alias;
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function binary($name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('binary', $name, $parameters);
    }

    /**
     * Execute the blueprint against the database.
     *
     * @param Connection $connection
     * @param Grammar    $grammar
     *
     * @return void
     */
    public function build(Connection $connection, Grammar $grammar)
    {
        foreach ($this->toSql($connection, $grammar) as $statement) {
            if ($connection->pretending()) {
                return;
            }

            $statement($this, $connection);
        }
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function date($name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('date', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function dateRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('date_range', $name, $parameters);
    }

    /**
     * @param string $name
     */
    public function document(string $name): void
    {
        $this->document = $name;
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function doubleRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('double_range', $name, $parameters);
    }

    /**
     * @param bool|string $value
     */
    public function dynamic($value): void
    {
        $this->addMetaField('dynamic', $value);
    }

    /**
     * @return void
     */
    public function enableAll(): void
    {
        $this->addMetaField('_all', ['enabled' => true]);
    }

    /**
     * @return void
     */
    public function enableFieldNames(): void
    {
        $this->addMetaField('_field_names', ['enabled' => true]);
    }

    /**
     * @param string $name
     *
     * @return PropertyDefinition
     */
    public function float($name, $total = 8, $places = 2, $unsuigned = false): PropertyDefinition
    {
        return $this->addColumn('float', $name);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function floatRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('float_range', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function geoPoint(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('geo_point', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function geoShape(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('geo_shape', $name, $parameters);
    }

    /**
     * @return string
     */
    public function getAlias(): string
    {
        return ($this->alias ?? $this->getTable()) . Config::get('database.connections.elasticsearch.suffix');
    }

    /**
     * @return string
     */
    public function getDocumentType(): string
    {
        return $this->document ?? Str::singular($this->getTable());
    }

    /**
     * @return string
     */
    public function getIndex(): string
    {
        $suffix    = Config::get('database.connections.elasticsearch.suffix');
        $timestamp = Carbon::now()->format('Y_m_d_His');
        return "{$timestamp}_{$this->getTable()}" . $suffix;
    }

    /**
     * @return array
     */
    public function getIndexSettings(): array
    {
        return $this->indexSettings;
    }

    /**
     * @return array
     */
    public function getMeta(): array
    {
        return $this->meta;
    }

    /**
     * @param string $name
     *
     * @return PropertyDefinition
     */
    public function integer($name, $autoIncrement = false, $unsigned = false): PropertyDefinition
    {
        return $this->addColumn('integer', $name);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function integerRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('integer_range', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function ip(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->ipAddress($name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function ipAddress($name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('ip', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function ipRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('ip_range', $name, $parameters);
    }

    /**
     * @param string $name
     * @param array  $relations
     *
     * @return PropertyDefinition
     */
    public function join(string $name, array $relations): PropertyDefinition
    {
        return $this->addColumn('join', $name, compact('relations'));
    }

    /**
     * @param string $name
     * @param array  $parameters
     * @return PropertyDefinition
     */
    public function keyword(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('keyword', $name, $parameters);
    }

    /**
     * @param string $name
     *
     * @return PropertyDefinition
     */
    public function long(string $name): PropertyDefinition
    {
        return $this->addColumn('long', $name);
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function longRange(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->range('long_range', $name, $parameters);
    }

    /**
     * @param array $meta
     */
    public function meta(array $meta): void
    {
        $this->addMetaField('_meta', $meta);
    }

    /**
     * @param string $name
     * @return PropertyDefinition|ColumnDefinition
     */
    public function nested(string $name)
    {
        return $this->addColumn('nested', $name);
    }

    /**
     * @param string  $name
     * @param Closure $parameters
     * @return PropertyDefinition|ColumnDefinition
     */
    public function object(string $name)
    {
        return $this->addColumn(null, $name);
    }

    /**
     * @param string $name
     * @param array  $parameters
     * @return PropertyDefinition|ColumnDefinition
     */
    public function percolator(string $name, array $parameters = [])
    {
        return $this->addColumn('percolator', $name, $parameters);
    }

    /**
     * @param string $type
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition|ColumnDefinition
     */
    public function range(string $type, string $name, array $parameters = [])
    {
        return $this->addColumn($type, $name, $parameters);
    }

    /**
     * @return void
     */
    public function routingRequired(): void
    {
        $this->addMetaField('_routing', ['required' => true]);
    }

    /**
     * @param string $name
     * @param null   $parameters
     * @return PropertyDefinition
     */
    public function string($name, $parameters = null)
    {
        return $this->text($name, $parameters ?? []);
    }

    /**
     * @param string     $name
     * @param array|null $parameters
     *
     * @return PropertyDefinition
     */
    public function text($name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('text', $name, $parameters);
    }

    /**
     * @param Connection $connection
     * @param Grammar    $grammar
     * @return array|Closure[]
     */
    public function toSql(Connection $connection, Grammar $grammar)
    {
        $this->addImpliedCommands($grammar);

        $statements = [];

        // Each type of command has a corresponding compiler function on the schema
        // grammar which is used to build the necessary SQL statements to build
        // the blueprint element, so we'll just call that compilers function.
        $this->ensureCommandsAreValid($connection);

        foreach ($this->commands as $command) {
            $method = 'compile' . ucfirst($command->name);
            if (is_callable([$grammar, $method]) && !is_null($statement = $grammar->$method($this, $command, $connection))) {
                $statements[] = $statement;
            }
        }

        return $statements;
    }

    /**
     * @param string $name
     * @param array  $parameters
     *
     * @return PropertyDefinition
     */
    public function tokenCount(string $name, array $parameters = []): PropertyDefinition
    {
        return $this->addColumn('token_count', $name, $parameters);
    }

    /**
     * @return Fluent
     */
    public function update()
    {
        return $this->addCommand('update');
    }
}
