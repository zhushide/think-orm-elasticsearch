<?php

namespace think\db;

use think\Paginator;

class Elasticsearch extends Query
{
    public function getPk()
    {
        return '_id';
    }

    public function __call($name, $arguments)
    {
        $param = $arguments[0] ?? [];
        $param['index'] = $this->getTable();
        return call_user_func([$this->connection->connect(), $name], $param);
    }

    public function paginate($listRows = null, $simple = false): Paginator
    {
        if (is_int($simple)) {
            $total  = $simple;
            $simple = false;
        }

        $defaultConfig = [
            'query'     => [], //url额外参数
            'fragment'  => '', //url锚点
            'var_page'  => 'page', //分页变量
            'list_rows' => 15, //每页数量
        ];

        if (is_array($listRows)) {
            $config   = array_merge($defaultConfig, $listRows);
            $listRows = intval($config['list_rows']);
        } else {
            $config   = $defaultConfig;
            $listRows = intval($listRows ?: $config['list_rows']);
        }

        $page = isset($config['page']) ? (int) $config['page'] : Paginator::getCurrentPage($config['var_page']);

        $page = $page < 1 ? 1 : $page;

        $config['path'] = $config['path'] ?? Paginator::getCurrentPath();

        if (!isset($total) && !$simple) {
            $data = $this->page($page, $listRows)->connection->query($this);
            $total = $this->count();
            $results = !empty($this->model) ? $this->resultSetToModelCollection($data['data']) : $data['data'];
        } elseif ($simple) {
            $results = $this->limit(($page - 1) * $listRows, $listRows + 1)->select();
            $total   = null;
        } else {
            $results = $this->page($page, $listRows)->select();
        }

        $this->removeOption('limit');
        $this->removeOption('page');

        return Paginator::make($results, $listRows, $page, $total, $simple, $config);
    }

    public function count(string $field = '*'): int
    {
        return $this->connection->count($this);
    }
}
