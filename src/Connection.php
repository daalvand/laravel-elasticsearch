<?php

namespace Daalvand\LaravelElasticsearch;

use Closure;
use Daalvand\LaravelElasticsearch\Database\Schema\Blueprint;
use Daalvand\LaravelElasticsearch\Database\Schema\ElasticsearchBuilder;
use Daalvand\LaravelElasticsearch\Database\Schema\Grammars\ElasticsearchGrammar;
use Daalvand\LaravelElasticsearch\Exceptions\BulkException;
use Daalvand\LaravelElasticsearch\Exceptions\QueryException;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Exception;
use Generator;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Arr;

class Connection extends BaseConnection
{
    /**
     * The Elasticsearch client.
     *
     * @var Client
     */
    protected Client $client;

    protected $indexSuffix = 'dev';

    protected float $requestTimeout = 300;

    public function getClient()
    {
        return $this->client;
    }

    /**
     * Create a new Elasticsearch connection instance.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        parent::__construct(null, '', '', $config);
        $this->indexSuffix = $config['suffix'] ?? '';
        // Extract the hosts from config
        $hosts = explode(',', $config['hosts'] ?? $config['host']);

        // You can pass options directly to the client
        $options = Arr::get($config, 'options', []);
        // Create the connection
        $this->client = $this->createClient($hosts, $config, $options);

        $this->useDefaultQueryGrammar();
        $this->useDefaultPostProcessor();
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param string $method
     * @param array  $parameters
     *
     * @return mixed
     */
    public function __call(string $method, array $parameters)
    {
        return $this->client->$method(...$parameters);
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param string $query
     * @param array  $bindings
     *
     */
    public function affectingStatement($query, $bindings = [])
    {
        //
    }

    /**
     * Start a new database transaction.
     *
     * @return void
     */
    public function beginTransaction()
    {
        //
    }

    /**
     * Commit the active database transaction.
     *
     * @return void
     */
    public function commit()
    {
        //
    }

    /**
     * @param string $index
     * @param string $name
     */
    public function createAlias(string $index, string $name): void
    {
        $this->client->indices()->putAlias(compact('index', 'name'));
    }

    /**
     * @param string $index
     * @param array  $body
     */
    public function createIndex(string $index, array $body): void
    {
        $this->client->indices()->create(compact('index', 'body'));
    }

    /**
     * Run a select statement against the database and return a generator.
     *
     * @param array $query
     * @param array $bindings
     * @param bool  $useReadPdo
     *
     * @return Generator
     */
    public function cursor($query, $bindings = [], $useReadPdo = false)
    {
        $chunk         = isset($query['size']) && $query['size'] < 100 ? $query['size'] : 100;
        $limit         = $query['size'] ?? 0;
        $query['size'] = $chunk;
        $readNums      = 0;
        do {
            $results    = $this->scroll($query);
            $numResults = count($results['hits']['hits']);
            $readNums   += $numResults;
            foreach ($results['hits']['hits'] as $result) {
                yield $result;
            }
            $query['scroll_id'] = $results['_scroll_id'];
        } while ($numResults && (!$limit || $readNums < $limit));
    }

    /**
     * Run a delete statement against the database.
     *
     * @param array $query
     * @param array $bindings
     *
     * @return array
     */
    public function delete($query, $bindings = [])
    {
        return $this->run(
            $query,
            $bindings,
            Closure::fromCallable([$this->client, 'deleteByQuery'])
        );
    }

    /**
     * Run a SQL statement and log its execution context.
     *
     * @param array   $query
     * @param array   $bindings
     * @param Closure $callback
     * @return mixed
     *
     * @throws \Illuminate\Database\QueryException
     */
    protected function run($query, $bindings, Closure $callback)
    {
        /** @noinspection PhpParamsInspection */
        return parent::run($query, $bindings, $callback);
    }

    /**
     * @param string $index
     */
    public function dropIndex(string $index): void
    {
        $this->client->indices()->delete(compact('index'));
    }

    /**
     * Get the timeout for the entire Elasticsearch request
     * @return float
     */
    public function getRequestTimeout(): float
    {
        return $this->requestTimeout;
    }

    /**
     * @return ElasticsearchBuilder
     */
    public function getSchemaBuilder()
    {
        return new ElasticsearchBuilder($this);
    }

    /**
     * @return ElasticsearchGrammar
     */
    public function getSchemaGrammar()
    {
        return new ElasticsearchGrammar();
    }

    /**
     * Get the table prefix for the connection.
     *
     * @return string
     */
    public function getTablePrefix()
    {
        return $this->indexSuffix;
    }

    /**
     * Run an insert statement against the database.
     *
     * @param array $params
     * @param array $bindings
     *
     * @return bool
     */
    public function bulk($params, $bindings = [])
    {
        return $this->run(
            $this->addClientParams($params),
            $bindings,
            Closure::fromCallable([$this->client, 'bulk'])
        );
    }

    /**
     * Log a query in the connection's query log.
     *
     * @param array      $query
     * @param array      $bindings
     * @param float|null $time
     *
     * @return void
     */
    public function logQuery($query, $bindings, $time = null)
    {
        $this->event(new QueryExecuted(json_encode($query), $bindings, $time, $this));

        if ($this->loggingQueries) {
            $this->queryLog[] = compact('query', 'bindings', 'time');
        }
    }

    /**
     * Prepare the query bindings for execution.
     *
     * @param array $bindings
     *
     * @return array
     */
    public function prepareBindings(array $bindings)
    {
        return $bindings;
    }

    /**
     * Execute the given callback in "dry run" mode.
     *
     * @param Closure $callback
     *
     */
    public function pretend(Closure $callback)
    {
        //
    }

    /**
     * Get a new raw query expression.
     *
     * @param mixed $value
     *
     */
    public function raw($value)
    {
        //
    }

    /**
     * Rollback the active database transaction.
     *
     * @return void
     */
    public function rollBack($toLevel = null)
    {
        //
    }

    /**
     * Run a select statement against the database using an Elasticsearch scroll cursor.
     *
     * @param array $query
     * @return array
     */
    public function scroll($query)
    {
        $scrollId = $query['scroll_id'] ?? null;
        $scroll   = $query['scroll'] ?? '10m';
        if ($scrollId === null) {
            return $this->select($query);
        }

        return $this->run(
            $this->addClientParams(['scroll_id' => $scrollId, 'scroll' => $scroll]),
            [],
            Closure::fromCallable([$this->client, 'scroll'])
        );
    }

    /**
     * Run a select statement against the database.
     *
     * @param array $params
     * @param array $bindings
     * @param bool  $useReadPdo
     * @return array
     */
    public function select($params, $bindings = [], $useReadPdo = true)
    {
        return $this->run(
            $this->addClientParams($params),
            $bindings,
            Closure::fromCallable([$this->client, 'search'])
        );
    }

    /**
     * Run a select statement and return a single result.
     *
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     */
    public function selectOne($query, $bindings = [], $useReadPdo = true)
    {
        //
    }

    /**
     * Set the table prefix in use by the connection.
     *
     * @param string $prefix
     *
     * @return void
     */
    public function setIndexSuffix($suffix)
    {
        $this->indexSuffix = $suffix;

        $this->getQueryGrammar()->setIndexSuffix($suffix);
    }

    /**
     * Get the timeout for the entire Elasticsearch request
     *
     * @param float $requestTimeout seconds
     *
     * @return self
     */
    public function setRequestTimeout(float $requestTimeout): self
    {
        $this->requestTimeout = $requestTimeout;

        return $this;
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param string         $query
     * @param array          $bindings
     * @param Blueprint|null $blueprint
     */
    public function statement($query, $bindings = [], Blueprint $blueprint = null)
    {
        //
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param string $table
     *
     */
    public function table($table, $as = null)
    {
        //
    }

    /**
     * Execute a Closure within a transaction.
     *
     * @param Closure $callback
     * @param int     $attempts
     *
     * @throws \Throwable
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        //
    }

    /**
     * Get the number of active transactions.
     *
     */
    public function transactionLevel()
    {
        //
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param string $query
     *
     */
    public function unprepared($query)
    {
        //
    }

    /**
     * Run an update statement against the database.
     *
     * @param array $query
     * @param array $bindings
     *
     * @return array
     */
    public function update($query, $bindings = [])
    {
        return $this->run(
            $query,
            $bindings,
            Closure::fromCallable([$this->client, 'index'])
        );
    }

    /**
     * @param string $index
     * @param array  $body
     */
    public function updateIndex(string $index, array $body): void
    {
        $this->client->indices()->putMapping(compact('index', 'body'));
    }

    /**
     * Set the table prefix and return the grammar.
     *
     * @param QueryGrammar $grammar
     *
     * @return QueryGrammar
     */
    public function withIndexSuffix(QueryGrammar $grammar)
    {
        $grammar->setIndexSuffix($this->indexSuffix);

        return $grammar;
    }

    /**
     * Add client-specific parameters to the request params
     *
     * @param array $params
     *
     * @return array
     */
    protected function addClientParams(array $params): array
    {
        if (isset($this->requestTimeout)) {
            $params['client']['timeout'] = $this->requestTimeout;
        }

        return $params;
    }

    /**
     * Create a new Elasticsearch client.
     *
     * @param array $hosts
     * @param array $config
     * @param array $options
     * @return Client
     * @noinspection PhpUnusedParameterInspection
     */
    protected function createClient($hosts, array $config, array $options)
    {

        // apply config to each host
        $hosts = array_map(function ($host) use ($config) {
            $port = !empty($config['port']) ? $config['port'] : 9200;

            $scheme = !empty($config['scheme']) ? $config['scheme'] : 'http';

            // force https for port 443
            $scheme = (int)$port === 443 ? 'https' : $scheme;

            return [
                'host'   => $host,
                'port'   => $port,
                'scheme' => $scheme,
                'user'   => !empty($config['username']) ? $config['username'] : null,
                'pass'   => !empty($config['password']) ? $config['password'] : null,
            ];
        }, $hosts);

        return ClientBuilder::create()
            ->setHosts($hosts)
            ->setSelector('\Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector')
            ->build();
    }

    /**
     * Get the default post processor instance.
     *
     * @return QueryProcessor
     */
    protected function getDefaultPostProcessor()
    {
        return new QueryProcessor();
    }

    /**
     * Get the default query grammar instance.
     *
     * @return QueryGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return $this->withIndexSuffix(new QueryGrammar);
    }

    /**
     * Run a search query.
     *
     * @param array   $query
     * @param array   $bindings
     * @param Closure $callback
     *
     * @return mixed
     *
     * @throws QueryException
     */
    protected function runQueryCallback($query, $bindings, Closure $callback)
    {
        try {
            $result = $callback($query, $bindings);
            if ($result['errors'] ?? false) {
                throw new BulkException($query, $bindings, $result);
            }
        } catch (Exception $e) {
            throw new QueryException($query, $e);
        }

        return $result;
    }
}
