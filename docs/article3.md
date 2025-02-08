## 1. 项目简介
**SmartETL**是一个**简单实用、灵活可配、开箱即用**的*Python数据处理（ETL）框架*，提供**Wikidata** / **Wikipedia** / **GDELT**等多种开源情报数据的处理流程； *支持大模型、API、常见文件、数据库*等多种输入输出及转换处理，支撑各类数据集成接入、大数据处理、离线分析计算、AI智能分析、知识图谱构建等任务。项目内置**50+**常用流程、**180+**常用ETL算子、**10+**领域特色数据处理流程，覆盖常见数据处理需求。

项目源码已经开放在[https://github.com/ictchenbo/SmartETL](https://github.com/ictchenbo/SmartETL)，感兴趣的小伙伴可以直接拉取源码进行试用，并且这个项目我会持续丰富完善，也欢迎大家提出提意见或是直接提出数据处理需求，也可以一起来参与项目建设。

本文是**《SmartETL：大模型赋能的开源情报数据处理框架》**系列第三篇，介绍SmartETL基于YAML格式的流程配置文件设计。

## 2. YAML文件格式简介
YAML文件，后缀名为".yaml"或".yml"，是一种目前比较流行的配置文件格式，比xml、json要简单方便很多，同时又比properties、config等文件强大。主要特点有以下几点：

1. 语法简单：通过`key: value`形式即可声明一个键值对，通过缩进语法支持数组和对象等复杂的值类型
2. 强类型：支持字符串、bool、数值、数组、dict，其中数组元素和dict的值又可以是字符串、bool、整数、数组、dict等，从而支持嵌套语法。由于yaml文件本身是文本文件，因此yaml加载器对值进行智能识别，从而区分不同类型。比如`true`、`yes`、`enabled`等字符串会映射成bool真值，`false`、`no`、`disabled`等会被映射成bool假值。
3. 表达能力与json等价，但写起来比json更简单。在json中字符串的Key和Value都需要使用引号包围，而且还需要考虑特殊字符需要使用转义字符，这些在yaml中就不需要，更加"所见即所得"。另外，对于数组和dict类型，也不需要包裹字符（[]{}）。以下分别是数组和dict的样例：
4. 支持注释。熟悉json的小伙伴知道，标准json不支持注释，因此既没办法对部分内容进行暂时屏蔽，也没办法添加适当的文档说明。在写接口文档的时候我会使用行位//的写法，但是仅用于文档，没法直接用于标准json加载器，比如下面这个例子是不符合标准json语法的（除非你自己实现一个json加载器）。
```yaml
consts:
  es1:
    index: goinv3_document_zhwiki
  remove_fields:
    - site
    - categories
    - sections
    - pageID
    - isDisambiguation
    - isRedirect
    - isStub
    - redirectTo
```

```json
{
  "entity_id": "Q999",//实体ID
  "name": "Artificial Intelligence", //实体名称
  "image_path": "./image/Q999.png", //头像
  "tags": ["热点技术", "新兴技术", "颠覆性技术"],//标签
  "name_en": "人工智能",//英文名称
  "domains": ["Computer Science", "Artificial Intelligence"],
  "text": "人工智能是机器具备人一样的思考、理解等能力的相关技术",
  "index": {//指数
    "hot": { "icon": "fire",  "value": 88, "label": "热度指数" },
    "new": { "icon": "new_tech",  "value": 89, "label": "新兴指数" },
    "inno": { "icon": "new_tech",  "value": 45, "label": "颠覆性指数" }
  }
}
```
但是yaml文件可以直接用 # 进行注释任意的行，yaml加载器会自动忽略以#开头的行。如下所示：
```yaml
nodes:
  print: Print
  count: Count
  buffer: Buffer(buffer_size=1000)
  flat: Flat
  rm: RemoveFields('golaxy_node_id', 'tech_domain')
  rename: RenameFields(golaxy_vocab_id='tech_id', name_zh='tech_name_ch', name_en='tech_name_en')
  tech_predict: MapUtil('landinn.tech_prediction.calc')
  select1: Select('golaxy_vocab_id', 'name_zh', 'name_en', 'predict.year', 'predict.historical_value', 'predict.predicted_value', 'predict.current_trends', 'predict.predicted_trends', 'predict.flag', short_key=True)
  rename1: RenameFields(golaxy_vocab_id='tech_id', name_zh='tech_name_ch', name_en='tech_name_en')
  write1: database.mysql.MySQL(**mysql_ld, table=result_table1)
  chain: Chain(tech_predict, select1, rename1, print, count)

#  disruptive_score: MapUtil('landinn.disruptive_score.calc')
#  write2: database.mysql.MySQL(**mysql_ld, table=result_table2)
#  chain: Chain(disruptive_score, print, count)
```
关于YAML文件的语法定义，可以参考[yaml.org](https://yaml.org/)。

YAML文件常见应用包括：**Docker Compose**；**Kubernetes**各类资源的定义；**Helm**的charts定义；**GIT CI/CD**配置文件；**Swagger/OpenAPI**；**nuclei** 模板；等等。考虑到YAML格式的广泛使用，在SmartETL支持YAML文件加载和输出。

## 3. YAML流程设计
SmartETL基于YAML格式进行ETL流程定义。
本项目中，把YAML文件中的顶层Key称为"指令"，通过几个指令组合来描述一个完整流程。

### 3.1 一个简单流程
以下是一个简单的例子：
```yaml
name: hello world
description: just for test
arguments: 1
consts:
  path: $PATH
fields:
  - id
  - url
nodes:
  select: Select(*fields)
  chain1: Chain(AddFields(chain='1'), Print())
  chain2: Chain(AddFields(chain='2'), Print())
loader: JsonLine(arg1)
processor: Chain(select, Fork(chain1, chain2, copy_data=True))
```
对应的流程示意图如下：
![流程示意图](https://i-blog.csdnimg.cn/direct/bdef8e0d222e4d008a45b86b894b4036.png#pic_center)
说明如下：

1. 流程名字`name`为"hello world"，执行流程时会打印此名称。
2. 流程描述`description`为"just for test"，所以这是一个测试流程。
3. 参数`arguments`为1，因此执行流程时需要传递一个命令行参数，通过`loader`定义可以发现，要求这个参数为待处理的json行文件的文件名（可能包括路径）。
4. `consts`声明了两个常量：`path`和`fields`，分别为字符串类型和数组类型，其中`path`引用了环境变量`PATH`的值；`fields`包含两项元素，即"id"和"url"。
5. `nodes`定义了三个流程节点：`select`、`chain1`和`chain2`。`select`定义为`Select(*fields)`，定义了仅选择`fields`字段，即仅保留数据中的"id"和"url"字段。`chain1`和`chain2`分别定义了两个处理流程，具体定义都包括两个步骤，第一步是添加一个名为`chain`的字段，第二步打印数据。
6. `loader`定义了流程数据源，按照json行文件格式读取参数文件
7. `processor`定义了流程数据处理过程，包含两个步骤：第一步为`select`，即移除输入数据中"id"和"url"以外的字段，第二步为一个`Fork`并行处理，基于`select`输出数据分别执行`chain1`和`chain2`。  

### 3.2 元数据指令
包括`name`、`description`。
- `name`：**str**【必需】流程的简洁名字，并具有一定的标识性
- `description`: **str**【可选】流程的描述，可以尽量详细，方便理解和查找

### 3.3 常量指令
包括`arguments`和`consts`，来源包括直接定义、命令行参数、环境变量。
- `arguments`: **int**【可选】流程参数数量，如果大于0，则流程执行时必须提供命令行参数。在流程节点中通过arg1，arg2，……方式进行引用。
- `consts`: **Any**【可选】直接定义常量，将需要再流程节点中使用的常量数据（如数据库连接参数、输入输出文件名等）进行统一定义方便修改维护。推荐将本地数据库的地址、用户密码等信息通过`consts`进行配置，方便具体节点定义时直接引用。`consts`支持嵌套定义，即值类型支持数组和dict（这正是YAML格式的优势）。此外，支持通过`$var`形式引用环境变量，例如`path: $PATH`将环境变量中的`PATH`值作为`path`常量进行定义，后续节点中使用`path`变量，即可访问`$PATH`。

### 3.4 节点指令
对流程中的节点进行定义，包括`nodes`、`loader`和`processor`。先介绍这三个指令，后文介绍节点的定义语法。

- `nodes`: **dict**[str, str]【可选】定义流程的处理节点，其中每个元素K-V为一个节点，并且后定义的节点可以引用先定义的节点，所有节点可以被loader和processor通过K直接引用。
- `loader`: **str**【必须】定义流程的加载器/数据源节点，通过访问该节点的iter方法提供流程所需要处理的数据。简单`loader`可直接定义，比如`JsonLine(arg1)`定义了一个节点，按照json行文件读取`arg1`对应文件。复杂`loader`可引用`nodes`中节点，如`TimedLoader(loader, interval=5)`定义了一个定时加载器，每间隔5秒去执行一次`loader`，这个具体的`loader`可以是获取数据库中最新数据或执行一次API请求，从而实现轮询效果。
- `processor`: **str**【必须】定义流程的处理节点，即处理过程。所有ETL可以简化成`loader->processor`过程，不过大部分情况下`processor`包含较多步骤逻辑复杂，需要引用`nodes`中节点，且大部分情况下数据处理为串行过程，SmartETL提供了`Chain`这一特殊节点类型，实现将多个具体处理节点进行链式串接，即前一个节点输出作为后一个节点的输入。`Chain`示意图如下，可定义为`Chain(t1, t2, t3, t4）`（假设`t1`、`t2`、`t3`、`t4`节点均已在nodes中定义）
![Chain流程示意图](https://i-blog.csdnimg.cn/direct/9e6f1123b9694c0da8551c2f4234bae6.png#pic_center)
另一种处理逻辑是分支并行处理，SmartETL提供了`Fork`这一特殊节点类型，表示多个处理子过程相互独立。`Fork`示意图如下，可定义为`Fork(t2, t3)`
![Fork流程示意图](https://i-blog.csdnimg.cn/direct/a9c8fd8f01ee4a84ad18ced3b84ad229.png#pic_center)
通过`Chain`和`Fork`可以实现大部分数据处理需求。未来还可以实现分支选择（If、Switch）、跳转（Jump）、分支汇合（Join）等。

### 3.5 流程引用指令
通过`from`进行流程引用，类似Docker `FROM`指令的功能。

流程构建时，将查看当前流程定义是否包含`from`指令，如果有，会递归加载相应的流程定义，最后跟当前流程进行合并，形成最终的流程定义。基于`from`指令可以实现流程节点复用，提供基础常量信息的统一管理维护，提高了流程定义的创建效率和可扩展性。

另一方面，当前`from`指令的实现方式为对yaml文件进行智能合并（考虑到`consts`可能包含数组值和dict值），暂未考虑到不同yaml文件的节点重名问题，节点会被下游流程定义的同名节点替换，可能导致上游流程功能逻辑被破坏。另外，在一些复杂数据处理场景中存在子流程需求，即将小流程（比如通过`from`定义的基础流程）作为一个整体进行处理（类似一个节点），甚至与大流程形成逻辑关联，这类场景SmartETL暂不支持。

### 3.6 节点定义
在`nodes`中的每个节点，以及`loader`、`processor`指令，设计了统一的节点定义语法。支持三种类型节点：

- 表达式定义节点，形如`name: =expr`，即以`=`作为前缀定义一个Python表达式。最常用场景是基于**Lambda**表达式定义一个函数，其他节点中基于这个函数进行处理（如`map`、`filter`算子）。此外，由于Python表达式不仅可以定义函数，还可以定义数据结构或对象，因此也可以用于定义常量，与`consts`区别在于需要通过执行表达式获得结果。
- 数据处理节点，形如`writer: database.MongoWriter(**mongo, buffer_size=1)`通过一种自定义的Python对象构造表达式，构造节点对象，该对象实现了数据处理接口。考虑到处理节点的多样性和扩展性，除了节点类名（如`MongoWriter`）以外，支持通过模块名（如`database`）对节点类名进行部分限定或完整限定。部分限定的情况主要用于SmartETL本身提供的处理算子；完整限定则用于用户自定义的处理算子，只要通过Python模块加载机制可以找到即可。
- 变量节点，采用与数据处理节点相同语法，但区别在于它是普通函数调用，因此该节点为函数执行结果，主要用于加载数据。例如，`props: util.sets.from_csv('config/props_human.txt')`这个节点通过执行`util.sets.from_csv`函数，实现从`config/props_human.txt`文件构造一个集合并命名为`props`，后续其他节点可以直接使用`props`这个集合，如通过`props`进行人的属性过滤`filter: wikidata.PropsFilter(props_set=props)`。

总的来说，大部分`nodes`节点和`processor`节点是数据处理节点，少部分`nodes`节点为表达式（定义函数或常量）节点或变量节点。考虑到`nodes`中大部分节点主要用于`processor`（ETL中的T和L），因此`nodes`定义中的部分限定模块会默认导向到`processor`对应模块，即`wikidata_filter.iterator`（代码模块名还没改）。为了提供用于`loader`的节点，要求其节点名称以"loader"开头，比如"loader1"或"loader_mysql"。之所以会这样，是因为在SmartETL设计中，将ETL算子分成了`loader`/`processor`/`util`三类，未来也许可以考虑将`loader`与`processor`合并到一个类型体系中。
