<?php

namespace CarroPublic\HeroSearch\Engines;

use Elasticsearch\Client;
use Laravel\Scout\Builder;
use Illuminate\Support\Arr;
use Laravel\Scout\Engines\Engine;
use Illuminate\Support\Facades\Artisan;

class ElasticSearchEngine extends Engine
{
    protected $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Update the given model in the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     * @return void
     */
    public function update($models)
    {
        $models->each(function ($model) {
            $params =  $this->getRequestBody($model, [
                'id'    => $model->id,
                'body'  => $model->toSearchableArray()
            ]);

            $this->client->index($params);
        });
    }

    /**
     * Remove the given model from the index.
     *
     * @param  \Illuminate\Database\Eloquent\Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $models->each(function ($model) {
            $params = $this->getRequestBody($model, [
                'id'    => $model->id
            ]);

            $this->client->delete($params);
        });
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder);
    }

    protected function performSearch(Builder $builder, array $options = [])
    {
        $params = array_merge_recursive($this->getRequestBody($builder->model),[
            // 'scroll' => '30s',
            'body'  => [
                'from' => 0,
                'size' => 5000,
                'query' => [
                    'bool' => [
                        'must' => [
                            'multi_match' => [
                                'query'     => $builder->query ?? '',
                                'fields'    => $this->getSearchableFields($builder->model),
                                'type'      => 'phrase_prefix',
                            ]
                        ]
                    ]
                ],
                'sort' => [
                    'id' => ['order' => 'desc']
                ]
            ]
        ], $options);

        if (empty($builder->query) && empty($builder->wheres)) {
            $params = array_merge_recursive($this->getRequestBody($builder->model),[
                // 'scroll' => '30s',
                'body'  => [
                    'from' => 0,
                    'size' => 5000,
                    'query' => [
                        'bool' => [
                            'must' => [
                                'match_all' => new \stdClass(),
                            ]
                        ]
                    ],
                    'sort' => [
                        'id' => ['order' => 'desc']
                    ]
                ]
            ], $options);
        }

        if (count($builder->wheres) > 0) {
            $data = [];
            foreach ($builder->wheres as $key => $value) {
                if ($value && is_array($value)) {
                    array_push($data, ['match' => [$key => $value[0]]]);
                } elseif ($value) {
                    array_push($data, ['match' => [$key => $value]]);
                }
            }

            $params['body']['query']['bool']['must'] = $data; // filter
        }

        if (count($builder->orders) > 0) {
            $params['body']['sort'] = collect($builder->orders)->map(function($value){
                return [$value['column'] => ["order" => $value['direction']]];
            })->toArray();
        }

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->client,
                $params
            );
        }

        return $this->client->search($params);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        return $this->performSearch($builder, [
            'from'  => ($page - 1) * $perPage,
            'size'  => $perPage
        ]);
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function map(Builder $builder, $results, $model)
    {
        if (count($hits = Arr::get($results, 'hits.hits')) === 0) {
            return $model->newCollection();
        };

        $objectIds = collect($hits)->pluck('_id')->values()->all();
        $objectIdPositions = array_flip($objectIds);

        return $model->getScoutModelsByIds(
            $builder, $objectIds
        )->filter(function ($model) use ($objectIds) {
            return in_array($model->getScoutKey(), $objectIds);
        })->sortBy(function ($model) use ($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        })->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return Arr::get($results, 'hits.total.value');
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function flush($model)
    {
        $this->client->indices()->delete([
            'index' => $model->searchableAs()
        ]);

        Artisan::call('hero-search:elasticsearch:create', [
            'model' => get_class($model)
        ]);
    }

    /**
     * Getting the request body of for index
     *
     * @param Model $model
     * @param array $options
     * @return array
     */
    private function getRequestBody($model, array $options = [])
    {
        return array_merge_recursive([
            'index' => $model->searchableAs(),
            'type'  => $model->searchableAs(),
        ], $options);
    }

    /**
     * Getting searchable fields of a model
     *
     * @return array
     */
    protected function getSearchableFields($model)
    {
        if (!method_exists($model, 'searchableFields')) {
            return [];
        }

        return $model->searchableFields();
    }
}
