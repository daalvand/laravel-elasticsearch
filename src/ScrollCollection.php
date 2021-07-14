<?php


namespace Daalvand\LaravelElasticsearch;


use Illuminate\Support\Collection as SopportCollection;

class ScrollCollection
{
    private SopportCollection $results;
    private string            $scrollId;
    private int             $total;

    /**
     * ScrollCollection constructor.
     * @param array  $results
     * @param string $scrollId
     * @param int    $total
     */
    public function __construct(array $results, string $scrollId, int $total)
    {
        $this->results  = collect($results);
        $this->scrollId = $scrollId;
        $this->total = $total;
    }

    /**
     * @return Collection
     */
    public function getResults(): SopportCollection
    {
        return $this->results;
    }

    /**
     * @return string
     */
    public function getScrollId(): string
    {
        return $this->scrollId;
    }

    /**
     * @return int
     */
    public function getTotal(): int
    {
        return $this->total;
    }

    /**
     * @param SopportCollection $results
     * @return ScrollCollection
     */
    public function setResults(SopportCollection $results): ScrollCollection
    {
        $this->results = $results;
        return $this;
    }
}