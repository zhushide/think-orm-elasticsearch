<?php

namespace think\db\builder;

use Closure;
use think\db\BaseQuery;

class Elasticsearch
{
    public function select(BaseQuery $query, bool $one = false): array
    {
        $options = $query->getOptions();
        $param = [
            'index' => $query->getTable(),
            'body' => ['query' => ['match_all' => new \stdClass]],
        ];

        if (!empty($options['where'])) {
            $param['body']['query'] = ['bool' => $this->parseWhere($query, $options['where'])];
        }

        if (!empty($options['order'])) {
            $param['body']['sort'] = $this->parseOrder($options['order']);
        }


        $limit = $one ? 1 : ($options['limit'] ?? null);
        if (!empty($limit)) {
            $arr = $this->parseLimit($limit);
            $param['from'] = intval($arr['from']);
            $param['size'] = intval($arr['size']);
        }

        if (!empty($options['group'])) {
            $param['body']['aggs'][$query->getTable()]['terms']['field'] = $options['group'];

            if (!empty($options['field'])) {
                foreach ($options['field'] as $key => $value) {
                    preg_match('/(\w+)\((\w+)\)/', is_numeric($key) ? $value : $key, $match);
                    if (isset($match[1]) && in_array($match[1], ['sum', 'max', 'min', 'avg'])) {
                        $param['body']['aggs'][$query->getTable()]['aggs'][$value][$match[1]]['field'] = $match[2];
                    }
                }
            }

            //分页
            if (!empty($param['size'])) {
                $param['body']['aggs'][$query->getTable()]['terms']['size'] = 65536;
                $param['body']['aggs'][$query->getTable()]['aggs']['bucket_truncate']['bucket_sort'] = ['from' => $param['from'], 'size' => $param['size']];
            }

            $param['size'] = 0;
        }

        if (!empty($options['field'])) {
            $param['body']['_source'] = $options['field'];
        }

        return $param;
    }

    protected function parseWhere(BaseQuery $query, array $where): array
    {
        $params = [];
        foreach ($where as $logic => $val) {
            $param = [];
            foreach ($val as $k => $v) {
                if ($v instanceof Closure) {
                    $newQuery = $query->newQuery();
                    $v($newQuery);
                    $param['must'][]['bool'] = $this->parseWhere($newQuery, $newQuery->getOptions('where'));
                } else {
                    switch (strtolower($v[1])) {
                        case '=':
                            $param['must'][]['term'][$v[0]] = $v[2];
                            break;
                        case 'in':
                            $param['must'][]['terms'][$v[0]] = $v[2];
                            break;
                        case 'not in':
                            $param['must_not'][]['terms'][$v[0]] = $v[2];
                            break;
                        case 'like':
                            $param['must'][]['wildcard'][$v[0]] = $v[2];
                            break;
                        case 'not like':
                            $param['must_not'][]['wildcard'][$v[0]] = $v[2];
                            break;
                        case '>':
                            $param['must'][]['range'][$v[0]]['gt'] = $v[2];
                            break;
                        case '<':
                            $param['must'][]['range'][$v[0]]['lt'] = $v[2];
                            break;
                        case '>=':
                            $param['must'][]['range'][$v[0]]['gte'] = $v[2];
                            break;
                        case '<=':
                            $param['must'][]['range'][$v[0]]['lte'] = $v[2];
                            break;
                        case '<>':
                            $param['must_not'][]['term'][$v[0]] = $v[2];
                            break;
                        case 'between':
                            [$min, $max] = explode(',', $v[2], 2);
                            $param['must'][] = [
                                'range' => [
                                    $v[0] => [
                                        'gte' => $min,
                                        'lte' => $max,
                                    ]
                                ]
                            ];
                            break;
                        case 'not between':
                            [$min, $max] = explode(',', $v[2], 2);
                            $param['must_not'][] = [
                                'range' => [
                                    $v[0] => [
                                        'gte' => $min,
                                        'lte' => $max,
                                    ]
                                ]
                            ];
                            break;
                        case 'null':
                            $param['must'][] = [
                                'missing' => ['field' => $v[0]]
                            ];
                            break;
                        case 'not null':
                            $param['must'][] = [
                                'exists' => ['field' => $v[0]]
                            ];
                            break;
                        default:
                            $param['must'][][strtolower($v[1])][$v[0]] = $v[2];
                            break;
                    }
                }
            }
            $params['should'][]['bool'] = $param;
        }

        return $params;
    }

    protected function parseLimit($limit): array
    {
        $limits = explode(',', $limit);
        return [
            'from' => isset($limits[1]) ? $limits[0] : 0,
            'size' => $limits[1] ?? $limits[0],
        ];
    }


    protected function parseOrder($order): array
    {
        $sort = [];
        foreach ($order as $key => $value) {
            if (is_int($key)) {
                $sort[$value] = 'asc';
            } else {
                $sort[$key] = $value;
            }
        }
        return $sort;
    }


    protected function parseGroup($group)
    {
    }
}
