<?php

namespace Daalvand\LaravelElasticsearch;

use Exception;
use Illuminate\Database\QueryException as BaseQueryException;
use Throwable;

class QueryException extends BaseQueryException
{
    public function __construct(array $sql, Throwable $previous)
    {
        parent::__construct(json_encode($sql), [], $previous);
    }

    /**
     * Format the error message.
     *
     * @param  array  $query
     * @param  array  $bindings
     * @param  Exception $previous
     * @return string
     */
    protected function formatMessage($query, $bindings, $previous)
    {
        return $previous->getMessage();
    }
}
