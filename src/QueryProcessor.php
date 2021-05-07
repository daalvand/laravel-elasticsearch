<?php

namespace Daalvand\LaravelElasticsearch;

use Illuminate\Database\Query\Processors\Processor as BaseProcessor;

class QueryProcessor extends BaseProcessor
{
    protected array   $rawResponse;
    protected array   $aggregations;
    protected int     $total;
    protected ?string $scrollId;
    private Builder   $query;

    /**
     * Process the results of a "select" query.
     *
     * @param Builder $query
     * @param array   $results
     * @return array
     */
    public function processSelect($query, $results)
    {
        $this->rawResponse  = $results;
        $this->aggregations = $results['aggregations'] ?? [];
        $this->total        = $results["hits"]["total"]["value"];
        $this->scrollId     = $results["_scroll_id"] ?? null;
        $this->query        = $query;
        $documents          = [];

        foreach ($results['hits']['hits'] as $result) {
            $documents[] = $this->documentFromResult($query, $result);
        }

        return $documents;
    }

    /**
     * Create a document from the given result
     *
     * @param Builder $query
     * @param array   $result
     * @return array
     */
    public function documentFromResult(Builder $query, array $result): array
    {
        $document          = $result['_source'];
        $document['id']    = $result['_id'];
        $document['_sort'] = $result['sort'] ?? null;
        if ($query->includeInnerHits && isset($result['inner_hits'])) {
            $document = $this->addInnerHitsToDocument($document, $result['inner_hits']);
        }

        return $document;
    }

    /**
     * Add inner hits to a document
     *
     * @param array $document
     * @param array $innerHits
     * @return array
     */
    protected function addInnerHitsToDocument($document, $innerHits): array
    {
        foreach ($innerHits as $documentType => $hitResults) {
            foreach ($hitResults['hits']['hits'] as $result) {
                $document['inner_hits'][$documentType][] = array_merge(['_id' => $result['_id']], $result['_source']);
            }
        }

        return $document;
    }

    /**
     * Get the raw Elasticsearch response
     *
     * @return array
     */
    public function getRawResponse(): array
    {
        return $this->rawResponse;
    }

    /**
     * Get the raw aggregation results
     *
     * @return array
     */
    public function getAggregationResults(): array
    {
        return $this->aggregations;
    }

    /**
     * @return int
     */
    public function getTotal(): int
    {
        return $this->total;
    }

    /**
     * @return string|null
     */
    public function getScrollId(): ?string
    {
        return $this->scrollId;
    }
}
