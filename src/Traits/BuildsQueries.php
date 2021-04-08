<?php

namespace Daalvand\LaravelElasticsearch\Traits;

trait BuildsQueries
{
    /**
     * Chunk the results of the query.
     *
     * @param  int  $count
     * @param  callable  $callback
     * @return bool
     */
    public function chunk($count, callable $callback)
    {
        $this->enforceOrderBy();

        $scrollId = null;

        do {
            if(isset($scrollId)){
                $this->scrollId($scrollId);
            }
            // We'll execute the query for the given page and get the results. If there are
            // no results we can just break and return from here. When there are results
            // we will call the callback with the current chunk of these results here.
            $results = $this->scrollTime('10m')->limit($count)->scroll();

            $countResults = $results->getResults()->count();

            if ($countResults === 0) {
                break;
            }

            // On each chunk result set, we will pass them to the callback and then let the
            // developer take care of everything within the callback, which allows us to
            // keep the memory low for spinning through large result sets for working.
            if ($callback($results->getResults(), $scrollId) === false) {
                return false;
            }

            $scrollId = $results->getScrollId();

            unset($results);
        } while ($countResults === $count);

        return true;
    }

}
