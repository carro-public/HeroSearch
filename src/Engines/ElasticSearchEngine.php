<?php

namespace CarroPublic\HeroSearch\Engines;

use Elasticsearch\Client;
use InvalidArgumentException;
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
            $params = $this->getRequestBody($model, [
                'id'   => $model->id,
                'body' => $model->toSearchableArray(),
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
                'id' => $model->id,
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

    /**
     * Perform search
     *
     * @param       \Laravel\Scout\Builder  $builder
     * @param       array $options
     *
     * @return      mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        // building query using Boolean query DSL:
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
        $boolMustArr = [];
        $sort        = [['id' => ['order' => 'desc']]]; // default sort order

        // check for query
        if (!empty($builder->query)) {
            $boolMustArr[] = [
                'multi_match' => [
                    'query'  => $builder->query ?? '',
                    'fields' => $this->getSearchableFields($builder->model),
                    'type'   => 'phrase_prefix',
                ],
            ];
        }

        // check for where conditions
        if (count($builder->wheres) > 0) {
            foreach ($builder->wheres as $key => $value) {
                if ($value && is_array($value)) {
                    $boolMustArr[] = ['match' => [$key => $value[0]]];
                } elseif ($value) {
                    $boolMustArr[] = ['match' => [$key => $value]];
                }
            }
        }

        // check for sort
        if (count($builder->orders) > 0) {
            $sort = collect($builder->orders)->map(function ($value) {
                return [$value['column'] => ['order' => $value['direction']]];
            })->toArray();
        }

        // create the request body
        $params = array_merge_recursive(
            $this->getRequestBody($builder->model, [
                'body' => [
                    'from'  => 0,
                    'size'  => 5000,
                    'query' => [
                        'bool' => [
                            'must' => $boolMustArr,
                        ],
                    ],
                    'sort'  => $sort,
                ],
            ]),
            $options
        );

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
            'from' => ($page - 1) * $perPage,
            'size' => $perPage,
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

        $objectIds         = collect($hits)->pluck('_id')->values()->all();
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
            'index' => $model->searchableAs(),
        ]);

        Artisan::call('hero-search:elasticsearch:create', [
            'model' => get_class($model),
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
            'index' => $this->getIndexName($model),
            'type'  => $model->searchableAs(),
        ], $options);
    }

    /**
     * Get the ES index name
     * 1st esIndexName will be given priority
     * 2nd herosearch.index_prefix
     * 3rd searchableAs as default
     * @param $model
     * @return string
     */
    private function getIndexName($model) {
        if (method_exists($model, 'esIndexName')) {
            return $model->esIndexName();
        }
        
        // If prefix is not null, append the $prefix before searchableAs
        $prefix = config('herosearch.index_prefix', null);
        if (!is_null($prefix)) {
            return $prefix . '_' . $model->searchableAs();
        }

        // Return searchableAs as default
        return $model->searchableAs();
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

    public function lazyMap(Builder $builder, $results, $model)
    {
        if (count($hits = Arr::get($results, 'hits.hits')) === 0) {
            return $model->newCollection();
        }

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

    public function createIndex($name, array $options = [])
    {
        if (isset($options['primaryKey'])) {
            throw new InvalidArgumentException('It is not possible to change the primary key name');
        }

        $this->client->indices()->create([
            'index' => $name,
            'body'  => [
                'settings' => array_merge_recursive([
                    'index' => [
                        'analysis'  => [
                            'filter' => [
                                'words_splitter' => [
                                    'catenate_all'      => 'true',
                                    'type'              => 'word_delimiter',
                                    'preserve_original' => 'true'
                                ]
                            ],
                            'analyzer' => [
                                'default' => [
                                    'filter'       => ['lowercase', 'words_splitter'],
                                    'char_filter'  => ['html_strip'],
                                    'type'         => 'custom',
                                    'tokenizer'    => 'standard'
                                ]
                            ]
                        ]
                    ]
                ], $options)
            ]
        ]);
    }

    public function deleteIndex($name)
    {
        $this->client->indices()->delete([
            'index' => $name
        ]);
    }
}
