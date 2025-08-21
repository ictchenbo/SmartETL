## 数据加载器（Loader）

模块：`smartetl.loader`

构造器：`<Comp>(*args, **kwargs)` 或 `<module>.<Comp>(*args, **kwargs)`

### 基类设计
1. 抽象基类 `DataLoader` 定义了数据加载器的接口
2. 文件基类 `file.File` 文件数据加载器
3. 二进制文件基类 `file.BinaryFile`
4. 文本文件基类`text.TextBase`

### 文件加载器
1. 按行读取文本文件 `Text(input_file, encoding="utf8")` 每行为字符串直接传递。
2. JSON行文件 `JsonLine(input_file, encoding="utf8")` 每行按照JSON进行解析并传递。
3. JSON数组文件 `JsonArray(input_file, encoding="utf8")` 整个文件为一个JSON数组，依次传递数组中的每个元素。
4. JSON文件 `Json(input_file, encoding="utf8")` 整个文件为一个JSON对象传递给后续节点。
5. JSON自由文件 `JsonFree(input_file, encoding="utf8")` 针对格式化json文件，自动检测JSON对象并传递给后续节点。
6. CSV文件 `CSV(input_file, sep: str = ',', with_header: bool = False, encoding='utf8')` 按照CSV文件进行解析，如果带有表头，则以字典结构进行传递，否则以单元格列表进行传递。
7. Excel文件流式 `xls.ExcelStream(input_file, sheets, with_header)` 基于openpyxl流式解析Excel（适合大文件）
8. Excel文件全量 `pandas.Excel(input_file, sheets, with_header)` 基于Pandas库解析Excel（全量加载，适合小文件）
9. PDF文件加载器 `pdf.PDF(input_file, max_pages=0)` 基于`pdfminer.six`读取PDF文件，每页文本作为一条数据
10. Word doc/docx文件加载器 `docx.Doc(input_file)` `docx.Docx(input_file)` 基于`python-docx`读取docx文件，doc则先通过`libreoffice`转换为docx，每个段落、表格作为一条数据
11. Parquet文件加载器 `parquet.Parquet(input_file)` 基于`pyarrow`读取parquet文件，每行作为一条数据
12. EML文件加载器 `eml.EML(input_file, tmp_dir: str = None, save_attachment=True)` 每封邮件一条数据
13. ppt/pptx文件加载器 `ppt.PPTX(input_file)` `ppt.PPTX(input_file)` 基于`python-pptx`读取pptx文件，ppt则先通过`libreoffice`转换为pptx，每个段落、表格作为一条数据
14. YAML文件 `Yaml(input_file, encoding="utf8")` 加载yaml文件，作为一个对象传递。
15. 纯文本文件 `TextPlain(input_file: str, encoding: str = "utf8", **kwargs)` 加载文本文件，作为一个字符串传递

### 文件夹加载器
通用文件夹加载 `Directory(folders, *suffix, recursive=False, type_mapping={}) `，参数说明：
- folders 指定文件或文件夹 
- *suffix 指定后缀名数组 如'.json' '.csv'，'all'表示全部支持的类型（此时其他参数会被忽略）
- recursive 进行递归处理，如果为True，会遍历子文件夹
- type_mapping 对文件类型进行映射 如`{'.json': '.jsonl'}`表示将`.json`文件当做`.jsonl`文件处理

已支持的文件类型（默认后缀名）：
- .txt -> Text
- .csv -> CSV
- .json -> Json
- .jsona -> JsonArray
- .jsonl -> JsonLine
- .jsonf -> JsonFree
- .xls -> xls.ExcelStream
- .xlsx -> xls.ExcelStream
- .doc -> docx.Doc
- .docx -> docx.Docx
- .eml -> eml.EML
- .parquet -> parquet.Parquet
- .pdf -> pdf.PDF

### Web API加载器
1. 基于HTTP加载URL（返回JSON的接口） `web.api.HttpBase(url: str, method: str = 'get', headers=None, auth=None, json=None, data=None)`
2. GET加载器（返回JSON的接口） `web.api.Get(url: str, headers=None, auth=None, json=None, data=None)`
3. POST加载器（返回JSON的接口） `web.api.Post(url: str, headers=None, auth=None, json=None, data=None)`
4. JsonP加载器 `web.jsonp.Jsonp(url: str, headers=None, auth=None, json=None, data=None)`


### 数据库加载器
提供常用数据库的数据查询式读取：
1. MySQL
2. PostgreSQL
3. ClickHouse
4. MongoDB
5. ElasticSearch
6. Kafka（基于web接口）

1. ClickHouse `database.CK` 实例化参数：
- host 服务器主机名 默认`"localhost"`
- tcp_port 服务器端口号 默认 `9000`
- username 用户名 默认`"default"`
- password 密码 默认`""`
- database 数据库 默认`"default"`
- table 表名 无默认值必须指定
- select 返回字段 默认`"*"` 示例`"id,name"`
- where 过滤条件 默认`None` 示例`a = '123' AND b is NULL` 
- limit 限制条件 默认`None` 示例`10,20`（即跳过10条返回20条）

示例配置：

```python
database.CK(host='10.208.57.5', port=59000, database='goin_kjqb_230202_v_3_0', table='entity_share_data_shard',
            select='mongo_id, name')
```

2. ElasticSearch `database.ESLoader` 实例化参数：
- host 服务器主机名 默认`"localhost"`
- port 服务器端口号 默认 `9200`
- user 用户名 默认`None`
- password 密码 默认`None`
- table 索引名 无默认值
- select 返回字段 默认`"*"` 示例`"id,name"`
- where 查询条件（参考[ES查询语法](http://)) 默认为`None`表示全部
- limit 限制数量（整数）

**注意**：由于ES常规检索方式限制最多返回10000条数据，因此limit>10000时采用scroll API。请参考：

示例配置：

```python
database.ES(host='10.208.57.13', table='docs')
```

3. MongoDB `MongoLoader` 实例化参数：
- host 服务器主机名 默认`"localhost"`
- port 服务器端口号 默认 `27017`
- username 用户名 默认`None`
- password 密码 默认`None`
- auth_db 认证数据库 默认`"admin"`
- database 数据库 默认`"default"`
- table 表名 无默认值必须指定
- select 返回字段 默认`"*"` 示例`"id,name"`
- where 查询条件（参考）默认为`None`表示全部，示例`{"name": "abc"}` 
- limit 限制数量（整数）

示例配置：

```python
database.Mongo(host='10.208.57.13', table='nodes')
```

4. MySQL `database.mysql.MySQL(host: str = 'localhost',
                 port: int = 3306,
                 user: str = "root",
                 password: str = None,
                 database: str = None,
                 paging: bool = False, **kwargs)`
5. PostgresSQL `database.postgres.PG(host: str = 'localhost',
                 port: int = 9000,
                 user: str = "default",
                 password: str = "",
                 database: str = 'default', **kwargs)`
6. Kafka `database.kafka.WebConsumer(api_base: str = 'localhost:62100',
                 group_id: str = None,
                 username: str = 'kafka',
                 user_auth: str = None,
                 topics: list = None,
                 auto_init: bool = False,
                 poll_timeout: int = 300,
                 poll_metabytes: int = 10,
                 poll_wait: int = 10,
                 connect_timeout: int = 30`
7. DBTables `database.meta.DBTables(loader, *database_list)` 基于已有的数据库loader读取当前可用的表格 表格返回名字和列的列表

### 其他加载器
1. 定时轮询加载器`TimedLoader` 可基于一个已有的加载器进行定时轮询 适合数据库轮询、服务监控等场景
2. 随机数生成器 `Random(num_of_times: int = 0)` 产生随机数（0~1）
3. 数组加载器 `Array(data: list)`
4. 字符串加载器 `String(text: str, sep: str = '\n')`
5. 函数加载器 `Function(function, *args, **kwargs)`

### 特定数据加载器：wikidata
提供两种wikidata文件格式加载器：
1. 全量dump文件：`wikidata.WikidataJsonDump` 本质上是一个每行一项的JSON数组文件 
2. 增量文件：`wikidata.WikidataXmlIncr` 本质上是wiki修订记录的XML文件的合并（即一个文件中包含了多个完整XML结构）

**说明**：
- 上述文件都支持以gz或bz2进行压缩，根据文件后缀名进行判断（分别为`.gz`和`.bz2`）
- wikidata Dump文件下载地址：https://dumps.wikimedia.org/wikidatawiki/entities/

**实例化参数**：文件路径；编码（默认为utf8）

### 特定数据加载器：GDELT
提供GDELT数据加载（通过网络下载） 每行提供url、file_size信息 配合对应iterator
1. 最近15分钟更新记录 `web.gdelt.GdeltLatest` 下载地址：http://data.gdeltproject.org/gdeltv2/lastupdate.txt
2. 全部文件，自2015年2月19日以来的全部更新记录 `web.gdelt.GdeltAll` 下载地址：http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
3. 从指定某个时间开始历史记录 `web.gdelt.GdeltTaskEmit(2024, 9, 1)` 自动保存最新时间戳 程序重启后可以从之前的点恢复

### 特定数据加载器：报告新闻
针对特定格式的word进行加载 `doc_news.News(input_file: str)`

