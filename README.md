# think-orm-elasticsearch

#### 介绍

基于think-orm的elasticsearch驱动，支持=、<>、>、>=、<、<=、[NOT] LIKE、[NOT] BETWEEN、[NOT] IN、[NOT] NULL表达式查询及其对应的快捷方法查询，支持table、name、where、whereOr、limit、page、order，不支持group、join

#### 软件架构

软件架构说明

#### 安装教程

composer require zhushide/think-orm-elasticsearch

#### 使用说明

1.  database.php配置文件新增一个connection  

```php
'elasticsearch' => [
    'type'=>'elasticsearch',
    'hosts' => env('elasticsearch.hosts', ['localhost:9200']),
    'retries' => env('elasticsearch.retries', 0), //重连次数
    'SSLVerification' => env('elasticsearch.SSLVerification', false),
    // 'logger' => app('log'),
],
```

2.  使用对应的connection操作ES  
```php
//单一文档索引
Db::connect('elasticsearch')->name('test')->insert(['id'=>1,'name'=>'hello world']);
//批量索引
Db::connect('elasticsearch')->name('test')->insertAll([['id'=>2,'name'=>'苹果'],['id'=>3,'name'=>'香蕉']]);
//更新文档
Db::connect('elasticsearch')->name('test')->where('id',1)->update(['name'=>'橘子']);
//删除文档
Db::connect('elasticsearch')->name('test')->where('id',1)->delete();
//获取文档
Db::connect('elasticsearch')->name('test')->where('id',1)->find();
//搜索文档
Db::connect('elasticsearch')->name('test')->where('name','苹果')->select();
//全文检索
Db::connect('elasticsearch')->name('test')->where('name','match','苹果 香蕉')->select();


//注意：模型里面使用需要关闭自动时间戳属性
```
