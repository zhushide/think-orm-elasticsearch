<?php

namespace think\db\connector;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use think\db\BaseQuery;
use think\db\Connection;
use think\db\builder\Elasticsearch as ElasticsearchBuild;
use think\db\Elasticsearch as ElasticsearchQuery;

class Elasticsearch extends Connection
{
    /**
     * @var ElasticsearchBuild
     */
    protected $builder;

    public function connect(array $config = [], $linkNum = 0): Client
    {
        if (isset($this->links[$linkNum])) {
            return $this->links[$linkNum];
        }

        if (empty($config)) {
            $config = $this->config;
        } else {
            $config = array_merge($this->config, $config);
        }

        $this->links[$linkNum] = ClientBuilder::fromConfig($config, true);

        return $this->links[$linkNum];
    }

    public function getBuilderClass()
    {
        return ElasticsearchBuild::class;
    }

    public function getQueryClass(): string
    {
        return ElasticsearchQuery::class;
    }

    public function select(BaseQuery $query): array
    {
        return $this->query($query)['data'];
    }

    public function find(BaseQuery $query): array
    {
        $options = $query->getOptions();
        if (isset($options['key'])) {
            return $this->connect()->getSource($this->getParams('GetSource', $options, ['index' => $query->getTable(), 'id' => $options['key']]));
        }

        return $this->query($query, true)['data'][0] ?? [];
    }

    public function insert(BaseQuery $query, bool $getLastInsID = false)
    {
        $options = $query->getOptions();

        $param = [];
        if (isset($options['data'][$query->getPk()])) {
            $param['id'] = $options['data'][$query->getPk()];
            unset($options['data'][$query->getPk()]);
        }
        $param += ['index' => $query->getTable(), 'body' => $options['data']];
        $result = $this->connect()->index($this->getParams('Index', $options, $param));

        return $getLastInsID ? $result['_id'] : 1;
    }

    public function insertAll(BaseQuery $query, array $dataSet = []): int
    {
        $index = $query->getTable();
        $pk = $query->getPk();
        $params = [];
        foreach ($dataSet as $data) {
            $params['body'][] = [
                'index' => [
                    '_index' => $index,
                ] + (isset($data[$pk]) ? ['_id' => $data[$pk]] : [])
            ];

            unset($data[$pk]);
            $params['body'][] = $data;
        }

        $responses = $this->connect()->bulk(array_merge($this->getParams([
            'wait_for_active_shards',
            'refresh',
            'routing',
            'timeout',
            'type',
            '_source',
            '_source_excludes',
            '_source_includes',
            'pipeline',
            'require_alias'
        ], $query->getOptions(), $params)));

        return count(array_filter(
            $responses['items'],
            function ($value) {
                return !empty($value['index']['_shards']['successful']);
            }
        ));
    }

    public function update(BaseQuery $query): int
    {
        $options = $query->getOptions();
        if (isset($options['key'])) {
            $params = [
                'index' => $query->getTable(),
                'id'    => $options['key'],
                'body'  => [
                    'doc' => $options['data']
                ]
            ];

            $response = $this->connect()->update($this->getParams('Update', $options, $params));
            return $response['_shards']['successful'] ?? 0;
        }

        $params = $this->builder->select($query);
        foreach ($options['data'] as $key => $value) {
            $script[] = "ctx._source.$key = params.$key";
        }
        $params['body']['script'] = ['source' => implode(';', $script), 'params' => $options['data']];
        return $this->connect()->updateByQuery($this->getParams('UpdateByQuery', $options, $params))['updated'] ?? 0;
    }

    public function delete(BaseQuery $query): int
    {
        $options = $query->getOptions();

        if (isset($options['key'])) {
            return $this->connect()->delete($this->getParams('Delete', $options, [
                'index' => $query->getTable(),
                'id' => $options['key']
            ]))['_shards']['successful'] ?? 0;
        }

        $params = $this->builder->select($query);
        return $this->connect()->deleteByQuery($this->getParams('DeleteByQuery', $options, $params))['deleted'] ?? 0;
    }

    public function value(BaseQuery $query, string $field, $default = null)
    {
        return $this->find($query)[$field] ?? $default;
    }

    public function column(BaseQuery $query, $column, string $key = ''): array
    {
        if (empty($key) || trim($key) === '') {
            $key = null;
        }

        if (\is_string($column)) {
            $column = \trim($column);
            if ('*' !== $column) {
                $column = \array_map('\trim', \explode(',', $column));
            }
        } elseif (\is_array($column)) {
            if (\in_array('*', $column)) {
                $column = '*';
            }
        }
        $field = $column;
        if ('*' !== $column && $key && !\in_array($key, $column)) {
            $field[] = $key;
        }

        $resultSet = $this->select($query->field($field));
        if (is_string($key) && strpos($key, '.')) {
            [$alias, $key] = explode('.', $key);
        }

        if (empty($resultSet)) {
            $result = [];
        } elseif ('*' !== $column && \count($column) === 1) {
            $column = \array_shift($column);
            if (\strpos($column, ' ')) {
                $column = \substr(\strrchr(\trim($column), ' '), 1);
            }

            if (\strpos($column, '.')) {
                [$alias, $column] = \explode('.', $column);
            }

            $result = \array_column($resultSet, $column, $key);
        } elseif ($key) {
            $result = \array_column($resultSet, null, $key);
        } else {
            $result = $resultSet;
        }

        return $result;
    }

    public function transaction(callable $callback)
    {
    }

    public function startTrans()
    {
    }

    public function commit()
    {
    }


    public function rollback()
    {
    }

    public function getLastSql(): string
    {
        return '';
    }

    public function close()
    {
    }

    public function query(BaseQuery $query, bool $one = false)
    {
        $query->parseOptions();

        $data = $this->connect()->search($this->getParams('Search', $query->getOptions(), $this->builder->select($query, $one)));
        $result = array_map(function ($value) {
            return $value['_source'] + ['_id' => $value['_id']];
        }, $data['hits']['hits'] ?? []);

        return ['count' => $data['hits']['total']['value'], 'data' => $result];
    }

    public function count(BaseQuery $query)
    {
        $params = $this->builder->select($query);
        return $this->connect()->count($this->getParams(
            'Count',
            $query->getOptions(),
            [
                'index' => $params['index'],
                'body' => [
                    'query' => $params['body']['query'],
                ]
            ]
        ))['count'] ?? 0;
    }

    protected function getParams($endpoint, array $options, array $params)
    {
        if (is_array($endpoint)) {
            $allows = $endpoint;
        } else {
            $class = '\\Elasticsearch\\Endpoints\\' . $endpoint;
            $allows = (new $class)->getParamWhitelist();
        }

        $query = array_merge($this->config['params'] ?? [], $options);
        return array_merge(array_intersect_key($query, array_flip($allows)), $params);
    }
}