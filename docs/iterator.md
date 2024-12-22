## 处理节点（Iterator）

模块：`wikidata_filter.iterator`

组件构造：`Comp(*args, **kwargs)` `<module>.<Comp>(*args, **kwargs)`


### 基类设计
1. 抽象基类 `JsonIterator` 定义了数据处理的接口
```python
class JsonIterator:
    def on_start(self):
        pass

    def on_data(self, data: Any, *args):
        pass

    def __process__(self, data: Any or None):
        pass
    
    def on_complete(self):
        pass

    @property
    def name(self):
        return self.__class__.__name__
```

提供`_set`方法，支持链式设置组件属性，如`Count()._set(ticks=100)._set(label='aaa')`

### 组合节点
1. 并行处理 `Fork(*nodes)`
2. 串行处理 `Chain(*nodes)`
3. 重复数据 `Repeat(num_of_repeats)`

### 基础类
1. 打印数据 `Print` 方便调试或日志记录 无参数
2. 计数 `Count(ticks=1000, label='-')` 对数据进行统计，方便观察 参数：ticks、label
3. 过滤 `Filter(matcher)` 参数：过滤函数/Matcher对象 可参考[Matcher](#matcher)

### Matcher（也是Filter）
1. 简单JSON匹配 `matcher.SimpleJsonMatch(**match_rules)`
2. 基于`jsonpath`语法规则的匹配 `matcher.JsonPathMatch(pattern)`

### 修改转换
1. 投影操作 `Select(*keys)` 支持嵌套字段 如`user.name`
2. 数据映射转换 `Map(mapper, key=None, target_key=None)` 如果指定了key，则是针对该字段的映射
3. 移除字段 `RemoveFields(*keys)`
4. 重命名字段 `RenameFields(**kwargs)`
5. 字段添加 `AddFields(**kwargs)` 仅添加不存在的字段
6. 字段更新 `UpdateFields(**kwargs)` Upsert模式
7. 字段填充 `InjectField(kv,inject_path, reference_path)`
8. 对调KV `ReverseKV()`
9. 基于规则的转换 `MapRules(rules)`
10. 字段扁平化 `Flat(key, flat_mode='value')` 通过flat_mode指定扁平化模式 支持对数组、字典类型的字段或整个输入进行扁平化
11. 扁平化转换 `FlatMap(mapper)` 对mapper结果进行扁平化
12. 字段作为JSON加载 `FieldJson(key)`
13. 复制字段 `CopyFields(*keys)` 复制已有的字段 如果目标字段名存在 则覆盖
14. 拼接字段 `ConcatFields(target_key,*source_keys, sep='_')` 将source_keys拼接作为target_key字段
15. 属性扁平化 `FlatProperty(*keys, inherit_props=False)` 提取指定属性进行返回
16. 多字段转换 `MapMulti(mapper, *keys, **kwargs)` 同时对多个字段的值应用mapper函数，通过*keys指定源-目标相同的字段，通过**kwargs指定源-目标不同的字段
17. 字段填充 `MapFill(cache, target_key, source_key=None)` 与`InjectField`效果相同，基于source_key的值在cache中查找，结果作为target_key字段。source_key为None，则使用target_key
18. 函数转换 `MapUtil(mod_name: str, *args, **kwargs)` 基于给定的函数对象完整限定名（如`wikidata_filter.util.db_util.dtype_mysql`，要求为一元函数，但可以接收其他预定义的值）加载函数对象作为`Map`的转换函数，可实现非常比较灵活的数据转换，可调用任意工具类函数

### 统计分析类
1. 按照某个字段对数据进行分组 `Group(by=key, emit_fast=True)` 对数据分组，然后再往后传递
2. 采样 `Sample(rate=0.01)` 
3. 去重 `Distinct(field)`

以下算子均需要搭配`Group`使用： 
1. 聚合统计分析基类 `aggs.Reduce(func, source_key='values', target_key=None)`  根据提供的函数进行数据规约
2. 自定义规约 `aggs.ReduceBy(init_func, add_func)` 提供一个初始函数和加法函数进行规约
3. 统计函数算子，包括`Count`、`Sum`、`Mean`、`Min`、`Max`、`Var`、`Std`，这些算子构造方法除了Count无需参数以外，其他均需要一个字段名参数
4. 分组整体处理算子，包括`Head(n)`、`Tail(n)`、`Sample(rate)`、`Distinct(field)`、`OrderBy(field, descend=False)`

### 缓冲处理基类
1. 基本缓冲类 收集一批数据再集中往后传递 `Buffer(buffer_size=1000,mode='batch')`
2. 缓冲写基类 用于进行数据库和文件带缓冲输出 `BufferedWriter(buffer_size=1000,mode='single')`
3. 分组处理 `Group(by, emit_fast=True)` 对数据进行分组，然后整组往后传递

### 输出文件
1. 文本文件(带缓冲) `WriteText(output_file: str, append: bool = False, encoding: str = "utf8", buffer_size: int = 1000, sep: str = '\n')`
2. JSON文件 `WriteJSON(output_file,append=False)`
3. CSV文件 `WriteCSV(output_file,keys=None,append=False)`

### 输出到数据库
1. ElasticSearch `database.ESWriter(host="localhost",port=9200,user=None,password=None,index=None,buffer_size=1000)`
2. ClickHouse `database.CKWriter(host='localhost',tcp_port=9000,username="default",password="",database='default',table=None, buffer_size=1000)`
3. MongoDB `database.MongoWriter(host='localhost',port=27017,username=None,password=None,database='default',auth_db='admin',collection=None,buffer_size=1000)`

### Wikidata处理
模块：`wikidata_filter.iterator.wikidata`

1. 生成ID-名称映射结构 `IDNameMap`
2. item字段简化，对labels/description/alias仅保留中文或英文 `Simplify`
3. 对item的属性进行简化  `SimplifyProps`
```textmate
    对原wikidata对象属性(claims)进行简化
    claims := dict[str, list[Prop]]
    Prop := {id: str, datatype: str, datavalue: PropVal,  qualifiers: dict[str, list[QProp]}
    QProp := {hash: str, datatype: PropDataType, datavalue: PropVal }
    PropDataType :=  wikibase-entityid | globecoordinate | quantity | time | monolingualtext | novalue | somevalue
```
4. 属性过滤器，仅保留具有特定属性（Pxx）的item `PropsFilter(props_set)`
5. 属性值过滤器，仅保留具有特定属性值（Qxx）的item `ValuesFilter(props_set)`
6. 属性对象名称注入，根据KV查询对象的名称进行注入 `ObjectNameInject(kv)`
7. item条目对应wikipedia页面摘要注入，根据KV查询维基页面摘要进行注入 `ObjectAbstractInject(kv)`
8. 对item的labels/descriptions/abstract字段转化为中文简体 `ChineseSimple`
9. 生成关系数据结构（**multiple**） `AsRelation`
10. Wikidata基本匹配 `matcher.WikidataMatcherV1(match_relations)`
11. 针对简化wikidata匹配 `matcher.WikidataMatcher(match_relations)` （也可以用`matcher.WikidataMatcherV2`）


### Wikidata知识图谱处理
模块：`wikidata_filter.iterator.wikidata`

1. 生成Item和Property摘要 `wikidata.Entity`
2. 生成实体Item：`wikidata.Entity` -> `Filter(lambda p: p['_type']=='item')`
3. 生成属性Property：`wikidata.Entity` -> `Filter(lambda p: p['_type']=='property')`
4. 提取实体属性：`wikidata.Property` （结果根据`_type`区分为：item_property property_property）
5. 生成实体关系：`wikidata.Relation`

### GDELT数据处理
模块：`wikidata_filter.iterator.web.gdelt`
1. 基于gdelt的更新文件（CSV.zip）及对应的schema文件生成json记录 `web.gdelt.Export(save_path=None)` save_path保存下载文件的路径

**说明** GDELT文件包括：
- 事件表 `event-table`（schema见`config/gdelt/export.schema`）
- 事件提到表 `mention-table` （schema见`config/gdelt/mention.schema`）
- 事件图谱 **TODO**


### 大模型处理
模块：`wikidata_filter.iterator.model`

提供了月之暗面（Kimi）、Siliconflow、天玑等模型/模型平台服务调用，作为数据处理算子
1. Siliconflow平台 `model.Siliconflow(api_key,field,proxy,model,prompt,ignore_errors=True)`
2. 天玑平台 `model.GoGPT(api_base,field,prompt,ignore_errors=True)` 
3. 向量化处理 `model.embed.Local(api_base: str, field: str, target_key: str = '_embed')` 调用向量化服务实现对指定文本字段生成向量。

### 自然语言处理
模块：`wikidata_filter.iterator.nlp`
1. 标签分割 `nlp.splitter.TagSplit(*keys)` 对指定字段进行分割处理，转换为标签数组
2. 文本分段（chunk化） `nlp.splitter.TextSplit(key, target_key, algorithm='simple')` 实现文本chunk化，便于建立向量化索引。


### OpenAPI格式转换
模块：`wikidata_filter.iterator.web.openapi`
1. 解析OpenAPI `web.openapi.FromOpenAPI`
2. 生成OpenAPI `web.openapi.ToOpenAPI`
