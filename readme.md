# HeroSearch

Laravel Scout elasticsearch driver base on our own needs.

## What's hero-search?

Before Carro, it was car hero. Base on that, we named our elastic-scout package as hero search.
Hero search is a scout package. We created this for our own needs. Currently, this was mostly inspred via [this course](https://codecourse.com/courses/create-a-laravel-scout-elasticsearch-driver). We will keep add the features base on our needs.

## Installation

Make sure you have installed [elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started-install.html)

```
$ composer require carropublic/herosearch
```

You should publish Scout and ElasticSearch configuration using:
```
$ php artisan vendor:publish --provider="Laravel\Scout\ScoutServiceProvider"
$ php artisan vendor:publish --provider="CarroPublic\HeroSearch\HeroSearchServiceProvider"
```

## Package Configuration

In your `.env` file, add host and port of your running elasticsearch.

```
ELASTICSEARCH_HOST=your_elasticsearch_host
ELASTICSEARCH_PORT=your_elasticsearch_port

# This prefix will be used to append before the index name
# To separate environement when sharing ES instance
ELASTICSEARCH_INDEX_PREFIX=prefix
```

Update scout driver to `elasticsearch` as well.
```
SCOUT_DRIVER=elasticsearch
```

## Usage example

Use scout `Searchable` trait in your model

```php
<?php

namespace App;

use Laravel\Scout\Searchable;

class User extends Model
{
    use Searchable;
    ...
}
```

Need to add index for your model that is needed to use scout searching
```
$ php artisan hero-search:elasticsearch:create path_to_your_model(Eg. App\\User)
```


Then import the records
```
$ php artisan scout:import path_to_your_model(Eg. App\\User)
```

You can also remove imported data by flush command:
```
$ php artisn scout:flush path_to_your_model(Eg. App\\User)
```

Add `searchableFields` methods in model to identify your query must be serached in which fields.
Eg:
```php
public function searchableFields()
{
    return [
        'name',
        'email'
    ];
}
```

Then can search by using:
```
Model::search($query)
```

Eg:
```
User::search('foo');
```

## Release History

## Contributing

 1. Fork it <https://github.com/carro-public/hero-search>
 2. Create your feature branch (git checkout -b feature/fooBar)
 3. Commit your changes (git commit -am 'Add some fooBar')
 4. Push to the branch (git push origin feature/fooBar)

## Security

If you discover any security related issues, please email aung.koko@carro.co instead of using the issue tracker.

## Credits

- All Contributors

## License

The MIT License (MIT). Please see License File for more information.
