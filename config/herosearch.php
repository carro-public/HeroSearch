<?php

return [
    'host'  => env('ELASTICSEARCH_HOST', '127.0.0.1'),
    'port'  => env('ELASTICSEARCH_PORT', '9200'),
    'index_prefix' => env('ELASTICSEARCH_INDEX_PREFIX', null)
];