<?php

namespace Daalvand\LaravelElasticsearch;

use DateTime;
use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;
use Illuminate\Support\Str;
use InvalidArgumentException;

class QueryGrammar extends BaseGrammar
{
    /**
     * The index suffix.
     */
    protected string $indexSuffix = '';

    /**
     * Compile a select statement
     *
     * @param Builder $builder
     * @return array
     */
    public function compileSelect($builder): array
    {
        $query = $this->compileWheres($builder);

        $params = [
            'index' => $builder->from . $this->indexSuffix,
            'body'  => [
                '_source' => $builder->columns && !in_array('*', $builder->columns, true) ? $builder->columns : true,
                'query'   => $query['query']
            ],
        ];

        if ($query['filter']) {
            $params['body']['query']['bool']['filter'] = $query['filter'];
        }

        if ($query['postFilter']) {
            $params['body']['post_filter'] = $query['postFilter'];
        }

        if ($builder->aggregations) {
            $params['body']['aggregations'] = $this->compileAggregations($builder);
        }

        // Apply order, offset and limit
        if ($builder->orders) {
            $params['body']['sort'] = $this->compileOrders($builder, $builder->orders);
        }

        if ($builder->offset) {
            $params['body']['from'] = $builder->offset;
        }

        if (isset($builder->limit)) {
            $params['body']['size'] = $builder->limit;
        }

        if ($builder->trackTotalHits ?? false) {
            $params['body']['track_total_hits'] = $builder->trackTotalHits;
        }

        if ($builder->searchAfter ?? null) {
            $params['body']['search_after'] = $builder->searchAfter;
        }

        if (isset($builder->scrollId)) {
            $params['scroll_id'] = $builder->scrollId;
        }

        if (isset($builder->scrollTime)) {
            $params['scroll'] = $builder->scrollTime;
        }

        if (!$params['body']['query']) {
            unset($params['body']['query']);
        }

        return $params;
    }

    /**
     * Compile where clauses for a query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileWheres($builder): array
    {
        $queryParts = [
            'query'      => 'wheres',
            'filter'     => 'filters',
            'postFilter' => 'postFilters'
        ];

        $compiled = [];

        foreach ($queryParts as $queryPart => $builderVar) {
            $clauses = $builder->$builderVar ?: [];

            $compiled[$queryPart] = $this->compileClauses($builder, $clauses);
        }

        return $compiled;
    }

    /**
     * Compile general clauses for a query
     *
     * @param Builder $builder
     * @param array   $clauses
     * @return array
     */
    protected function compileClauses(Builder $builder, array $clauses): array
    {
        $query = [];
        $isOr  = false;

        foreach ($clauses as $where) {
            // We use different methods to compile different wheres
            $method = 'compileWhere' . $where['type'];
            $result = $this->{$method}($builder, $where);

            // Wrap the result with a bool to make nested wheres work
            if ($where['boolean'] !== 'or' && count($clauses) > 0) {
                $result = ['bool' => ['must' => [$result]]];
            }

            // If this is an 'or' query then add all previous parts to a 'should'
            if (!$isOr && $where['boolean'] === 'or') {
                $isOr = true;

                if ($query) {
                    $query = [
                        'bool' => [
                            'should' => [$query]
                        ]
                    ];
                } else {
                    $query['bool']['should'] = [];
                }
            }

            // Add the result to the should clause if this is an Or query
            if ($isOr) {
                $query['bool']['should'][] = $result;
            } else {
                // Merge the compiled where with the others
                /** @noinspection SlowArrayOperationsInLoopInspection */
                $query = array_merge_recursive($query, $result);
            }
        }

        return $query;
    }

    /**
     * Compile a general where clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereBasic(Builder $builder, array $where): array
    {
        $value        = $this->getValueForWhere($builder, $where);
        $operatorsMap = [
            '>'  => 'gt',
            '>=' => 'gte',
            '<'  => 'lt',
            '<=' => 'lte',
        ];

        if ($where['operator'] === 'exists' || is_null($value)) {
            $query = [
                'exists' => [
                    'field' => $where['column'],
                ],
            ];

            $where['not'] = !$value;
        } else if (array_key_exists($where['operator'], $operatorsMap)) {
            $operator = $operatorsMap[$where['operator']];
            $query    = [
                'range' => [
                    $where['column'] => [
                        $operator => $value,
                    ],
                ],
            ];
        } else {
            $query = [
                'term' => [
                    $where['column'] => $value,
                ],
            ];
        }

        $query = $this->applyOptionsToClause($query, $where);

        if (!empty($where['not'])
            || ($where['operator'] === '!=' && !is_null($value))
            || ($where['operator'] === '=' && is_null($value))
        ) {
            $query = [
                'bool' => [
                    'must_not' => [
                        $query,
                    ],
                ],
            ];
        }

        return $query;
    }

    /**
     * Compile a date clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereDate(Builder $builder, array $where): array
    {
        if ($where['operator'] === '=') {
            $value = $this->getValueForWhere($builder, $where);

            $where['value'] = [$value, $value];

            return $this->compileWhereBetween($builder, $where);
        }

        return $this->compileWhereBasic($builder, $where);
    }

    /**
     * Compile a nested clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereNested(Builder $builder, array $where): array
    {
        $compiled = $this->compileWheres($where['query']);

        foreach ($compiled as $queryPart => $clauses) {
            $compiled[$queryPart] = array_map(function ($clause) use ($where) {
                if ($clause) {
                    $this->applyOptionsToClause($clause, $where);
                }

                return $clause;
            }, $clauses);
        }

        $compiled = array_filter($compiled);

        return reset($compiled);
    }

    /**
     * Compile a relationship clause
     *
     * @param Builder $builder
     * @param array   $where
     * @param string  $relationship
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function applyWhereRelationship(Builder $builder, array $where, string $relationship): array
    {
        $compiled = $this->compileWheres($where['value']);

        $relationshipFilter = "has_{$relationship}";
        $type               = $relationship === 'parent' ? 'parent_type' : 'type';

        // pass filter to query if empty allowing a filter interface to be used in relation query
        // otherwise match all in relation query
        if (empty($compiled['query'])) {
            $compiled['query'] = empty($compiled['filter']) ? ['match_all' => (object)[]] : $compiled['filter'];
        } else if (!empty($compiled['filter'])) {
            throw new InvalidArgumentException('Cannot use both filter and query contexts within a relation context');
        }

        $query = [
            $relationshipFilter => [
                $type   => $where['documentType'],
                'query' => $compiled['query'],
            ],
        ];

        $query = $this->applyOptionsToClause($query, $where);

        return $query;
    }

    /**
     * Compile a parent clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereParent(Builder $builder, array $where): array
    {
        return $this->applyWhereRelationship($builder, $where, 'parent');
    }

    /**
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereParentId(Builder $builder, array $where): array
    {
        return [
            'parent_id' => [
                'type' => $where['relationType'],
                'id'   => $where['id'],
            ],
        ];
    }

    /**
     * @param Builder $builder
     * @param array   $where
     * @return array[]
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWherePrefix(Builder $builder, array $where): array
    {
        $query = [
            'prefix' => [
                $where['column'] => $where['value'],
            ]
        ];

        return $query;
    }

    /**
     * Compile a child clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     *
     */
    protected function compileWhereChild(Builder $builder, array $where): array
    {
        return $this->applyWhereRelationship($builder, $where, 'child');
    }

    /**
     * Compile an in clause
     *
     * @param Builder $builder
     * @param array   $where
     * @param bool    $not
     * @return array
     */
    protected function compileWhereIn(Builder $builder, array $where, $not = false): array
    {
        $column = $where['column'];
        $values = $this->getValueForWhere($builder, $where);

        $query = [
            'terms' => [
                $column => array_values($values),
            ],
        ];

        $query = $this->applyOptionsToClause($query, $where);

        if ($not) {
            $query = [
                'bool' => [
                    'must_not' => [
                        $query,
                    ],
                ],
            ];
        }

        return $query;
    }

    /**
     * Compile a not in clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereNotIn(Builder $builder, array $where): array
    {
        return $this->compileWhereIn($builder, $where, true);
    }

    /**
     * Compile a null clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereNull(Builder $builder, array $where): array
    {
        $where['operator'] = 'exists';

        return $this->compileWhereBasic($builder, $where);
    }

    /**
     * Compile a not null clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereNotNull(Builder $builder, array $where): array
    {
        $where['operator'] = 'exists';

        return $this->compileWhereBasic($builder, $where);
    }

    /**
     * Compile a where between clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     */
    protected function compileWhereBetween(Builder $builder, array $where): array
    {
        $column = $where['column'];
        $values = $this->getValueForWhere($builder, $where);

        if ($where['not']) {
            $query = [
                'bool' => [
                    'should' => [
                        [
                            'range' => [
                                $column => [
                                    'lte' => $values[0],
                                ],
                            ],
                        ],
                        [
                            'range' => [
                                $column => [
                                    'gte' => $values[1],
                                ],
                            ],
                        ],
                    ],
                ],
            ];
        } else {
            $query = [
                'range' => [
                    $column => [
                        'gte' => $values[0],
                        'lte' => $values[1]
                    ],
                ],
            ];
        }

        return $query;
    }

    /**
     * Compile where for function score
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereFunctionScore(Builder $builder, array $where): array
    {
        $cleanWhere = $where;

        unset(
            $cleanWhere['function_type'],
            $cleanWhere['type'],
            $cleanWhere['boolean']
        );

        $query = [
            'function_score' => [
                $where['function_type'] => $cleanWhere
            ]
        ];

        return $query;
    }

    /**
     * Compile a query_string clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereQueryString(Builder $builder, array $where): array
    {
        $options = $where['options'] ?? [];
        $main    = ['query' => $where['value']];
        return [
            'query_string' => array_merge($options, $main),
        ];
    }


    /**
     * Compile a wildcard clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereWildcard(Builder $builder, array $where): array
    {
        $options = $where['options'] ?? [];
        $main    = [
            'value'            => $where['value'],
            "boost"            => $options["boost"] ?? 1.0,
            "rewrite"          => $options["rewrite"] ?? "constant_score",
            "case_insensitive" => $options["case_insensitive"] ?? false
        ];
        return [
            'wildcard' => [$where['column'] => $main],
        ];
    }

    /**
     * Compile a wildcard clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereRegexp(Builder $builder, array $where): array
    {
        $options = $where['options'] ?? [];
        $main    = [
            'value'                   => $where['value'],
            "flags"                   => $options["flags"] ?? "ALL",
            "case_insensitive"        => $options["case_insensitive"] ?? false,
            "max_determinized_states" => $options["max_determinized_states"] ?? 10000,
            "rewrite"                 => $options["rewrite"] ?? "constant_score"
        ];
        return [
            'regexp' => [$where['column'] => $main],
        ];
    }


    /**
     * Compile a search clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereSearch(Builder $builder, array $where): array
    {
        $fields = '_all';

        if (!empty($where['options']['fields'])) {
            $fields = $where['options']['fields'];
        }

        if (is_array($fields) && !is_numeric(array_keys($fields)[0])) {
            $fieldsWithBoosts = [];

            foreach ($fields as $field => $boost) {
                $fieldsWithBoosts[] = "{$field}^{$boost}";
            }

            $fields = $fieldsWithBoosts;
        }

        if (is_array($fields) && count($fields) > 1) {
            $type = $where['options']['matchType'] ?? 'most_fields';

            $query = [
                'multi_match' => [
                    'query'  => $where['value'],
                    'type'   => $type,
                    'fields' => $fields,
                ],
            ];
        } else {
            $field = is_array($fields) ? reset($fields) : $fields;

            $query = [
                'match' => [
                    $field => [
                        'query' => $where['value'],
                    ]
                ],
            ];
        }

        if (!empty($where['options']['fuzziness'])) {
            $matchType = array_keys($query)[0];

            if ($matchType === 'multi_match') {
                $query[$matchType]['fuzziness'] = $where['options']['fuzziness'];
            } else {
                $query[$matchType][$field]['fuzziness'] = $where['options']['fuzziness'];
            }
        }

        if (!empty($where['options']['constant_score'])) {
            $query = [
                'constant_score' => [
                    'query' => $query,
                ],
            ];
        }

        return $query;
    }

    /**
     * Compile a script clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereScript(Builder $builder, array $where): array
    {
        return [
            'script' => [
                'script' => array_merge($where['options'], ['source' => $where['script']]),
            ],
        ];
    }

    /**
     * Compile a geo distance clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereGeoDistance(Builder $builder, array $where): array
    {
        $query = [
            'geo_distance' => [
                'distance'       => $where['distance'],
                $where['column'] => $where['location'],
            ],
        ];

        return $query;
    }

    /**
     * Compile a where geo bounds clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereGeoBoundsIn(Builder $builder, array $where): array
    {
        return [
            'geo_bounding_box' => [
                $where['column'] => $where['bounds'],
            ],
        ];
    }

    /**
     * Compile a where nested doc clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereNestedDoc(Builder $builder, $where): array
    {
        $wheres = $this->compileWheres($where['query']);

        $query = [
            'nested' => [
                'path' => $where['column']
            ],
        ];

        $query['nested'] = array_merge($query['nested'], array_filter($wheres));

        if (isset($where['operator']) && $where['operator'] === '!=') {
            $query = [
                'bool' => [
                    'must_not' => [
                        $query
                    ]
                ]
            ];
        }

        return $query;
    }

    /**
     * Compile a where not clause
     *
     * @param Builder $builder
     * @param array   $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileWhereNot(Builder $builder, array $where): array
    {
        return [
            'bool' => [
                'must_not' => [
                    $this->compileWheres($where['query'])['query']
                ]
            ]
        ];
    }

    /**
     * Get value for the where
     *
     * @param Builder $builder
     * @param array   $where
     * @return mixed
     * @noinspection PhpUnusedParameterInspection
     */
    protected function getValueForWhere(Builder $builder, array $where)
    {
        switch ($where['type']) {
            case 'In':
            case 'NotIn':
            case 'Between':
                $value = $where['values'];
                break;

            case 'Null':
                $value = false;
                break;
            case 'NotNull':
                $value = true;
                break;

            default:
                $value = $where['value'];
        }

        // Convert DateTime values to UTCDateTime.
        if ($value instanceof DateTime) {
            $value = $this->convertDateTime($value);
        }

        return $value;
    }

    /**
     * Apply the given options from a where to a query clause
     *
     * @param array $clause
     * @param array $where
     * @return array
     */
    protected function applyOptionsToClause(array $clause, array $where)
    {
        if (empty($where['options'])) {
            return $clause;
        }

        $optionsToApply = ['boost', 'inner_hits'];
        $options        = array_intersect_key($where['options'], array_flip($optionsToApply));

        foreach ($options as $option => $value) {
            $method = 'apply' . Str::studly($option) . 'Option';

            if (method_exists($this, $method)) {
                $clause = $this->$method($clause, $value, $where);
            }
        }

        return $clause;
    }

    /**
     * Apply a boost option to the clause
     *
     * @param array $clause
     * @param mixed $value
     * @param array $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function applyBoostOption(array &$clause, $value, $where): array
    {
        $firstKey = key($clause);

        if ($firstKey !== 'term') {
            return $clause[$firstKey]['boost'] = $value;
        }

        $key = key($clause['term']);

        $clause['term'] = [
            $key => [
                'value' => $clause['term'][$key],
                'boost' => $value
            ]
        ];

        return $clause;
    }

    /**
     * Apply inner hits options to the clause
     *
     * @param array $clause
     * @param mixed $value
     * @param array $where
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function applyInnerHitsOption(array $clause, $value, $where): array
    {
        $firstKey = key($clause);

        $clause[$firstKey]['inner_hits'] = empty($value) || $value === true ? (object)[] : (array)$value;

        return $clause;
    }

    /**
     * Compile all aggregations
     *
     * @param Builder $builder
     * @return array
     */
    protected function compileAggregations(Builder $builder): array
    {
        $aggregations = [];

        foreach ($builder->aggregations as $aggregation) {
            $result = $this->compileAggregation($builder, $aggregation);

            $aggregations[] = $result;
        }
        return array_merge(...$aggregations);
    }

    /**
     * Compile a single aggregation
     *
     * @param Builder $builder
     * @param array   $aggregation
     * @return array
     * @noinspection PhpUnusedParameterInspection
     */
    protected function compileAggregation(Builder $builder, array $aggregation): array
    {
        $key = $aggregation['key'];

        $method = 'compile' . ucfirst(Str::camel($aggregation['type'])) . 'Aggregation';
        if (method_exists($this, $method)) {
            $compiled = [
                $key => $this->$method($aggregation)
            ];
        } else {
            $compiled = [
                Str::snake($aggregation['type']) => $aggregation['args']
            ];
        }

        if (isset($aggregation['aggregations']) && $aggregation['aggregations']->aggregations) {
            $compiled[$key]['aggregations'] = $this->compileAggregations($aggregation['aggregations']);
        }

        return $compiled;
    }

    /**
     * Compile filter aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileFilterAggregation(array $aggregation): array
    {
        $filter = $this->compileWheres($aggregation['args']);

        $filters = $filter['filter'] ?? [];
        $query   = $filter['query'] ?? [];

        $allFilters = array_merge($query, $filters);

        return [
            'filter' => $allFilters ?: ['match_all' => (object)[]]
        ];
    }

    /**
     * Compile nested aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileNestedAggregation(array $aggregation): array
    {
        $path = is_array($aggregation['args']) ? $aggregation['args']['path'] : $aggregation['args'];

        return [
            'nested' => [
                'path' => $path
            ]
        ];
    }

    /**
     * Compile terms aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileTermsAggregation(array $aggregation): array
    {
        $field = is_array($aggregation['args']) ? $aggregation['args']['field'] : $aggregation['args'];

        $compiled = [
            'terms' => [
                'field' => $field
            ]
        ];

        $allowedArgs = [
            'collect_mode',
            'exclude',
            'execution_hint',
            'include',
            'min_doc_count',
            'missing',
            'order',
            'script',
            'show_term_doc_count_error',
            'size',
        ];

        if (is_array($aggregation['args'])) {
            $validArgs         = array_intersect_key($aggregation['args'], array_flip($allowedArgs));
            $compiled['terms'] = array_merge($compiled['terms'], $validArgs);
        }

        return $compiled;
    }


    /**
     * Compile cardinality aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileCardinalityAggregation(array $aggregation): array
    {
        $field = is_array($aggregation['args']) ? $aggregation['args']['field'] : $aggregation['args'];

        $compiled = ['cardinality' => ['field' => $field]];

        if ($aggregation['precision_threshold'] ?? null) {
            $compiled['cardinality']['precision_threshold'] = $aggregation['precision_threshold'];
        }
        return $compiled;
    }

    /**
     * Compile date histogram aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileDateHistogramAggregation(array $aggregation): array
    {
        if (is_array($aggregation['args'])) {
            return ['date_histogram' => $aggregation['args']];
        }
        return ['date_histogram' => ['field' => $aggregation['args']]];
    }

    /**
     * Compile date range aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileDateRangeAggregation(array $aggregation): array
    {
        return [
            'date_range' => $aggregation['args']
        ];
    }

    /**
     * Compile exists aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileExistsAggregation(array $aggregation): array
    {
        $field = is_array($aggregation['args']) ? $aggregation['args']['field'] : $aggregation['args'];

        return [
            'exists' => [
                'field' => $field
            ]
        ];
    }

    /**
     * Compile sum aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileSumAggregation(array $aggregation): array
    {
        return $this->compileMetricAggregation($aggregation);
    }

    /**
     * Compile avg aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileAvgAggregation(array $aggregation): array
    {
        return $this->compileMetricAggregation($aggregation);
    }

    /**
     * Compile metric aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileMetricAggregation(array $aggregation): array
    {
        $metric = $aggregation['type'];

        $field = is_array($aggregation['args']) ? $aggregation['args']['field'] : $aggregation['args'];

        return [
            $metric => [
                'field' => $field
            ]
        ];
    }

    /**
     * Compile children aggregation
     *
     * @param array $aggregation
     * @return array
     */
    protected function compileChildrenAggregation(array $aggregation): array
    {
        $type = is_array($aggregation['args']) ? $aggregation['args']['type'] : $aggregation['args'];

        return [
            'children' => [
                'type' => $type
            ]
        ];
    }

    /**
     * Compile the orders section of a query
     *
     * @param Builder $builder
     * @param array   $orders
     * @return array
     */
    protected function compileOrders($builder, $orders = []): array
    {
        return collect($orders)->pluck('direction', 'column')->toArray();
    }

    /**
     * Compile the given values to an Elasticsearch insert statement
     *
     * @param Builder $builder
     * @param array   $values
     * @return array
     */
    public function compileInsert($builder, array $values): array
    {
        $params = [];
        foreach ($values as $doc) {
            $this->cleanDoc($doc);
            if (isset($doc['child_documents'])) {
                foreach ($doc['child_documents'] as $childDoc) {
                    $this->cleanDoc($childDoc);
                    $params['body'][] = [
                        'index' => [
                            '_index' => $builder->from . $this->indexSuffix,
                            '_id'    => $childDoc['id'],
                            'parent' => $doc['id'],
                        ]
                    ];

                    $params['body'][] = $childDoc['document'];
                }

                unset($doc['child_documents']);
            }

            $index = [
                '_index' => $builder->from . $this->indexSuffix,
                '_id'    => $doc['id'],
            ];
            $this->checkRoutingOfDoc($builder, $index, $doc);
            $this->checkParentOfDoc($builder, $index, $doc);
            $params['body'][] = ['index' => $index];
            $params['body'][] = $doc;
        }
        $params = array_merge($params, $builder->getOptions());
        return $params;
    }

    /**
     * Compile the given values to an Elasticsearch bulk update statement
     *
     * @param Builder $builder
     * @param array   $values
     * @param bool    $upsert
     * @return array
     */
    public function compileUpdateByIds($builder, array $values, bool $upsert = false)
    {
        $params = [];
        foreach ($values as $doc) {
            $this->cleanDoc($doc);
            if (isset($doc['child_documents'])) {
                foreach ($doc['child_documents'] as $childDoc) {
                    $this->cleanDoc($childDoc);
                    $params['body'][] = [
                        'update' => [
                            '_index' => $builder->from . $this->indexSuffix,
                            '_id'    => $childDoc['id'],
                            'parent' => $doc['id'],
                        ]
                    ];
                    $params['body'][] = [
                        'doc'           => $childDoc['document'],
                        'doc_as_upsert' => $upsert
                    ];
                }
                unset($doc['child_documents'], $doc['_sort']);
            }

            $update = [
                '_index' => $builder->from . $this->indexSuffix,
                '_id'    => $doc['id'],
            ];
            $this->checkRoutingOfDoc($builder, $update, $doc);
            $this->checkParentOfDoc($builder, $update, $doc);

            $params['body'][] = ['update' => $update];
            $params['body'][] = ['doc' => $doc, 'doc_as_upsert' => $upsert];
        }
        $params = array_merge($params, $builder->getOptions());
        return $params;
    }


    /**
     * Compile a delete query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileDelete($builder): array
    {
        $clause = $this->compileSelect($builder);
        $clause = array_merge($clause, $builder->getOptions());
        return $clause;
    }

    /**
     * Convert a key to an Elasticsearch-friendly format
     *
     * @param mixed $value
     * @return string
     */
    protected function convertKey($value): string
    {
        return (string)$value;
    }

    /**
     * Compile a delete query
     *
     * @param $value
     * @return string
     */
    protected function convertDateTime($value): string
    {
        if (is_string($value)) {
            return $value;
        }

        return $value->format($this->getDateFormat());
    }

    /**
     * @inheritdoc
     */
    public function getDateFormat(): string
    {
        return 'Y-m-d\TH:i:s';
    }

    /**
     * Get the grammar's index suffix.
     *
     * @return string
     */
    public function getIndexSuffix(): string
    {
        return $this->indexSuffix;
    }

    /**
     * Set the grammar's table suffix.
     *
     * @param string $suffix
     * @return $this
     */
    public function setIndexSuffix(string $suffix): self
    {
        $this->indexSuffix = $suffix;

        return $this;
    }

    /**
     * add routing to index or update of bulk
     * @param Builder $builder
     * @param array   $index
     * @param         $doc
     */
    protected function checkRoutingOfDoc(Builder $builder, array &$index, &$doc): void
    {
        if (isset($doc['_routing'])) {
            $index['routing'] = $doc['_routing'];
            unset($doc['_routing']);
        } elseif ($routing = $builder->getRouting()) {
            $index['routing'] = $routing;
        }
    }

    /**
     * add parent to index or update of bulk
     * @param Builder $builder
     * @param array   $update
     * @param         $doc
     */
    protected function checkParentOfDoc(Builder $builder, array &$update, &$doc): void
    {
        if ($parentId = $builder->getParentId()) {
            $update['parent'] = $parentId;
        } else if (isset($doc['_parent'])) {
            $update['parent'] = $doc['_parent'];
            unset($doc['_parent']);
        }
    }

    /**
     * @param $doc
     */
    private function cleanDoc(&$doc): void
    {
        if (is_array($doc) && !empty($doc)) {
            unset($doc['_sort']);
        }
    }
}
