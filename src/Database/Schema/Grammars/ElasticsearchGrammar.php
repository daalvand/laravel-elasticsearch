<?php

namespace Daalvand\LaravelElasticsearch\Database\Schema\Grammars;

use Daalvand\LaravelElasticsearch\Connection;
use Daalvand\LaravelElasticsearch\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Blueprint as BaseBlueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;
use Illuminate\Support\Str;
use InvalidArgumentException;

/**
 * Class Grammar
 * @package Daalvand\LaravelElasticsearch\Database\Schema
 */
class ElasticsearchGrammar extends Grammar
{
    /** @var array */
    protected array $modifiers = ['Boost', 'Dynamic', 'Fields', 'Format', 'Index', 'Properties'];

    /**
     * @param Blueprint  $blueprint
     * @param Fluent     $command
     * @param Connection $connection
     * @noinspection PhpUnusedParameterInspection
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command, Connection $connection): void
    {
        $body = [
            'mappings' => array_merge(['properties' => $this->getColumns($blueprint)], $blueprint->getMeta()),
        ];

        if ($settings = $blueprint->getIndexSettings()) {
            $body['settings'] = $settings;
        }

        $connection->createIndex(
            $index = $blueprint->getIndex(), $body
        );

        $alias = $blueprint->getAlias();

        if (!$connection->getClient()->indices()->existsAlias(['name' => $alias])) {
            $connection->createAlias($index, $alias);
        }
    }

    /**
     * @param Blueprint  $blueprint
     * @param Fluent     $command
     * @param Connection $connection
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command, Connection $connection):void
    {
        $this->compileDropIfExists($blueprint, $command, $connection);
    }

    /**
     * @param Blueprint  $blueprint
     * @param Fluent     $command
     * @param Connection $connection
     * @noinspection PhpUnusedParameterInspection
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command, Connection $connection):void
    {
        $indices = $connection->getClient()->cat()->indices();
        foreach ($indices as $index) {
            if ($index && Str::contains($index['index'], $blueprint->getTable())) {
                $connection->dropIndex($index['index']);
            }
        }
    }

    /**
     * @param Blueprint  $blueprint
     * @param Fluent     $command
     * @param Connection $connection
     * @noinspection PhpUnusedParameterInspection
     */
    public function compileUpdate(Blueprint $blueprint, Fluent $command, Connection $connection): void
    {
        $connection->updateIndex(
            $blueprint->getAlias(),
            array_merge(['properties' => $this->getColumns($blueprint)], $blueprint->getMeta())
        );
    }

    /**
     * @inheritDoc
     */
    protected function addModifiers($sql, BaseBlueprint $blueprint, Fluent $property)
    {
        foreach ($this->modifiers as $modifier) {
            if (method_exists($this, $method = "modify{$modifier}")) {
                $property = $this->{$method}($blueprint, $property);
            }
        }

        return $property;
    }

    /**
     * @param Blueprint $blueprint
     * @param Fluent    $property
     *
     * @return Fluent
     * @noinspection PhpUnusedParameterInspection
     */
    protected function format(Blueprint $blueprint, Fluent $property)
    {
        if (!is_string($property->format)) {
            throw new InvalidArgumentException('Format modifier must be a string', 400);
        }

        return $property;
    }

    /**
     * @param BaseBlueprint $blueprint
     *
     * @return array
     */
    protected function getColumns(BaseBlueprint $blueprint)
    {
        $columns = [];

        foreach ($blueprint->getAddedColumns() as $property) {
            // Pass empty string as we only need to modify the property and return it.
            $column = $this->addModifiers('', $blueprint, $property);
            $key = Str::snake($column->name);
            unset($column->name);

            $columns[$key] = $column->toArray();
        }

        return $columns;
    }

    /**
     * @param Blueprint $blueprint
     * @param Fluent    $property
     * @return Fluent
     * @noinspection PhpUnusedParameterInspection
     */
    protected function modifyBoost(Blueprint $blueprint, Fluent $property)
    {
        if (!is_null($property->boost) && !is_numeric($property->boost)) {
            throw new InvalidArgumentException('Boost modifier must be numeric', 400);
        }

        return $property;
    }

    /**
     * @param Blueprint $blueprint
     * @param Fluent    $property
     *
     * @return Fluent
     * @noinspection PhpUnusedParameterInspection
     */
    protected function modifyDynamic(Blueprint $blueprint, Fluent $property)
    {
        if (!is_bool($property->dynamic) && !is_null($property->dynamic)) {
            throw new InvalidArgumentException('Dynamic modifier must be a boolean', 400);
        }

        return $property;
    }

    /**
     * @param Blueprint $blueprint
     * @param Fluent    $property
     *
     * @return Fluent
     * @noinspection PhpUnusedParameterInspection
     */
    protected function modifyFields(Blueprint $blueprint, Fluent $property)
    {
        if (!is_null($property->fields)) {
            $fields = $property->fields;
            $fields($blueprint = $this->createBlueprint());

            $property->fields = $this->getColumns($blueprint);
        }

        return $property;
    }

    /**
     * @param Blueprint $blueprint
     * @param Fluent    $property
     *
     * @return Fluent
     * @noinspection PhpUnusedParameterInspection
     */
    protected function modifyProperties(Blueprint $blueprint, Fluent $property)
    {
        if (!is_null($property->properties)) {
            $properties = $property->properties;
            $properties($blueprint = $this->createBlueprint());

            $property->properties = $this->getColumns($blueprint);
        }

        return $property;
    }

    /**
     * @return Blueprint
     */
    private function createBlueprint(): Blueprint
    {
        return new Blueprint('');
    }
}
