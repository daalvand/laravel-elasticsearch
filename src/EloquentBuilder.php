<?php

namespace Daalvand\LaravelElasticsearch;

use Daalvand\LaravelElasticsearch\Traits\BuildsQueries;
use Generator;
use Illuminate\Database\Eloquent\Builder as BaseBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Arr;
use InvalidArgumentException;

/**
 * Class EloquentBuilder
 * @method Builder filter($filters) Support for Searchable::scopeFilter()
 * @method Builder|self wildcard(string $column, string $query, $options = [], $boolean = 'and')
 *  @method Builder|self regexp(string $column, string $query, $options = [], $boolean = 'and')
 *  @method Builder|self queryString($query, $options = [], $boolean = 'and')
 * @package Daalvand\LaravelElasticsearch
 */
class EloquentBuilder extends BaseBuilder
{
    use BuildsQueries;

    /**
     * The base query builder instance.
     *
     * @var Builder
     */
    protected $query;

    protected $type;

    /**
     * this method used for deep paginate
     * @param array $sorts
     * @return $this
     */
    public function searchAfter(array $sorts): self
    {
        $this->query->searchAfter($sorts);
        return $this;
    }

    /**
     * if set it true you can see count of all documents
     * @param bool $trackTotalHits
     * @return self
     */
    public function trackTotalHits(bool $trackTotalHits): self
    {
        $this->query->trackTotalHits($trackTotalHits);
        return $this;
    }




    /**
     * Set a model instance for the model being queried.
     *
     * @param Model $model
     * @return $this
     */
    public function setModel($model): self
    {
        $this->model = $model;
        $this->query->from($model->getSearchIndex());
        $this->query->setType($model->getSearchType());
        return $this;
    }

    /**
     * @return EloquentBuilder|Model
     */
    public function getModel()
    {
        return parent::getModel();
    }

    /**
     * @inheritdoc
     */
    public function hydrate(array $items)
    {
        $instance = $this->newModelInstance();

        return $instance->newCollection(array_map(function ($item) use ($instance) {
            return $instance->newFromBuilder($item, $this->getConnection()->getName());
        }, $items));
    }

    public function getAggregationResults()
    {
        return $this->applyScopes()->query->getAggregationResults();
    }

    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     * @return Collection|static[]
     */
    public function get($columns = ['*'])
    {
        $builder = $this->applyScopes();

        $models = $builder->getModels($columns);

        // If we actually found models we will also eager load any relationships that
        // have been specified as needing to be eager loaded, which will solve the
        // n+1 query issue for the developers to avoid running a lot of queries.
        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $builder->getModel()->newCollection($models);
    }


    /**
     * Execute the query as a "select" statement.
     *
     * @param array $columns
     * @return ScrollCollection
     */
    public function scroll($columns = ['*'])
    {
        $builder = $this->applyScopes();
        $result = $this->query->scroll($columns);
        /** @noinspection PhpUndefinedMethodInspection */
        $models = $this->model->hydrate($result->getResults()->all())->all();

        // If we actually found models we will also eager load any relationships that
        // have been specified as needing to be eager loaded, which will solve the
        // n+1 query issue for the developers to avoid running a lot of queries.
        if (count($models) > 0) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $result->setResults($builder->getModel()->newCollection($models));
    }


    /**
     * @param string $columns
     * @return int
     */
    public function count($columns = '*'): int
    {
        return $this->toBase()->getCountForPagination($columns);
    }

    /**
     * @param string $collectionClass
     * @return Collection
     */
    public function getAggregations(string $collectionClass = ''): Collection
    {
        $collectionClass = $collectionClass ?: Collection::class;
        $aggregations    = $this->query->getAggregationResults();

        return new $collectionClass($aggregations);
    }

    /**
     * Get a generator for the given query.
     *
     * @return Generator
     */
    public function cursor()
    {
        foreach ($this->applyScopes()->query->cursor() as $record) {
            yield $this->model->newFromBuilder($record);
        }
    }

    /**
     * Paginate the given query.
     *
     * @param int      $perPage
     * @param array    $columns
     * @param string   $pageName
     * @param int|null $page
     * @return \Illuminate\Contracts\Pagination\LengthAwarePaginator
     *
     * @throws InvalidArgumentException
     */
    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null)
    {
        $page = $page ?: Paginator::resolveCurrentPage($pageName);

        $perPage = $perPage ?: $this->model->getPerPage();

        $results = $this->forPage($page, $perPage)->get($columns);

        $total = $this->toBase()->getCountForPagination($columns);

        return new LengthAwarePaginator($results, $total, $perPage, $page, [
            'path'     => Paginator::resolveCurrentPath(),
            'pageName' => $pageName,
        ]);
    }


    public function upsert(array $values, $uniqueBy = null, $update = null)
    {
        return parent::upsert($values, $uniqueBy, $update);
    }

    public function updateByIds(array $values)
    {
        $values = Arr::isAssoc($values) ? [$values] : $values;
        foreach ($values as $index => $value) {
            $values[$index] = $this->addUpdatedAtColumn($value);
        }
        return $this->toBase()->updateByIds($values);
    }


    /**
     * Add the "updated at" column to an array of values.
     *
     * @param array $values
     * @return array
     */
    protected function addUpdatedAtColumn(array $values)
    {
        if (!$this->model->usesTimestamps() ||
            is_null($this->model->getUpdatedAtColumn())) {
            return $values;
        }

        $column = $this->model->getUpdatedAtColumn();

        $values                   = array_merge(
            [$column => $this->model->freshTimestampString()],
            $values
        );
        $qualifiedColumn          = $column;
        $values[$qualifiedColumn] = $values[$column];
        unset($values[$column]);
        return $values;
    }
}
