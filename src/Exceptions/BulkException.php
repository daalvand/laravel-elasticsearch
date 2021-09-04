<?php

namespace Daalvand\LaravelElasticsearch\Exceptions;

use Exception;
use JsonException;

class BulkException extends Exception
{

    /**
     * @param array $query
     * @param array $bindings
     * @throws JsonException
     */
    public function __construct($query, $bindings, $result)
    {
        $queryStr    = is_array($query) ? json_encode($query, JSON_THROW_ON_ERROR) : (string)$query;
        $bindingsStr = is_array($bindings) ? json_encode($bindings, JSON_THROW_ON_ERROR) : (string)$bindings;
        $resultStr   = is_array($result) ? json_encode($result, JSON_THROW_ON_ERROR) : (string)$result;
        $message     = "Query:: $queryStr, Bindings :: $bindingsStr, Result:: $resultStr";
        parent::__construct($message);
    }
}