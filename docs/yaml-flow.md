## Yaml Flow
基于YAML文件格式定义的数据处理流程

### 字段说明
1. `name: str` 【必需】流程名称
2. `version: str` 流程版本号
3. `author: str` 作者
4. `description: str` 流程描述
5. `arguments: int` 流程接受的运行时参数个数 如果提供参数不足将报错。通过arg1,arg2,...进行引用
6. `consts: Any` 可用于组件的常量数据 支持通过$<VAR> 引用环境变量
7. `nodes: dict` 处理节点组件（包括动态变量定义 后定义的变量可引用前面定义的变量） 支持python表达式
8. `loader: str` 【必须】数据加载器组件，可引用`nodes`中已定义节点或创建新的节点
9. `processor: str` 【必须】数据处理器组件，通过引用`nodes`中变量定义主流程
10. `from: str or list` 集成的其他流程定义，支持单个文件或一组文件，如果文件不存在或出现循环引用将报错


总的来说，本框架实现的就是从`loader`加载数据 并通过`processor`进行处理

### from
功能：指定当前流程继承的流程，其值为一个或多个（数组）流程文件路径。

假设现有`base.yaml`文件如下：
```yaml
name: base flow
consts:
  mapping:
    姓名: name
    年龄: age
    性别: sex

loader: JsonLine('person.jsonl')
```

另一个流程定义文件`load_json.yaml`继承自`base.yaml`：
```yaml
from: base.yaml
description: 查看
consts:
  mapping:
    性别: gender
nodes:
  rename: RenameFields(mapping)

processor: Chain(Print(), rename, Print())
```

通过`from`指令，会对`load_json.yaml`与`base.yaml`进行合并，合并结果如下：
```yaml
name: base flow
description: 查看
consts:
  mapping:
    姓名: name
    年龄: age
    性别: gender

loader: JsonLine('person.jsonl')

nodes:
  rename: RenameFields(mapping)

processor: Chain(Print(), rename, Print())
```

注意：`consts`支持字典数据结构，通过`from`对consts进行合并时，相同字段会被覆盖。

### consts
功能：定义可用于流程节点的常量数据。在节点参数复杂情况下非常有用。

`consts`支持所有yaml数据类型，包括数值、字符串、字典、数组等。

### nodes
功能：以字典（str->str）形式定义流程的计算节点或数据节点。
有3种类型节点：
1. 普通节点/对象节点：通过类构造函数调用生成的对象节点，主要用于数据处理。如果需要用于数据加载，节点名字需要以`loader`开头。
2. 数据计算节点：提供函数调用表达式，将函数执行结果作为当前节点的值。常用于配置文件读取、字典加载等。
3. 表达式计算节点：形如`=expr`，`expr`表达式执行结果作为当前节点的值。常用于数据结构动态定义、Lambda函数定义。

### logging
功能：支持日志功能
- print_mode 针对`print`的处理
- name 日志名称
- log_level 级别 默认info
- log_file 输出文件 默认 `smartetl.log`

### limit
功能：限定处理的次数 可方便流程开发调试

### loader
功能：定义流程的数据源节点（目前仅支持单个数据源节点）。节点定义可引用nodes节点。


### processor
功能：定义流程的处理节点。节点定义可引用nodes节点。由于大部分数据处理为链式处理，因此经常用Chain进行流程组装。

1. 最简单的情况下，可直接引用nodes中定义的节点或直接初始化一个处理节点。
```yaml
nodes:
  print: Print
processor: print
```

与以下写法等价：
```yaml
processor: Print
```

2. 大多数情况下，通过Chain将多个处理节点串联。
```yaml
nodes:
  select: Select('id', 'name', 'description')
  rename: RenameFields(description='desc')
  print: Print
  count: Count(label='num')
processor: Chain(select, rename, print, count)
```

3. 某些情况下，需要进行并行处理，通过Fork定义要并行的节点
```yaml
nodes:
  print: Print
  rename1: RenameFields(description='desc')
  add1: AddFields(type=1)
  rename2: RenameFields(description='_desc')
  add2: AddFields(type=2)
  chain1: Chain(rename1, add1, print)
  chain2: Chain(rename2, add2, print)

processor: Fork(chain1, chain2, copy_data=True)
```

4. Chain和Fork类型节点可以自由组合，但不能循环引用。除了简单节点，通常也不建议重复引用，否则容易产生逻辑错误。
```yaml
nodes:
  print: Print
  select: Select('id', 'name', 'description')
  rename1: RenameFields(description='desc')
  add1: AddFields(type=1)
  rename2: RenameFields(description='_desc')
  add2: AddFields(type=2)
  chain1: Chain(rename1, add1, print)
  chain2: Chain(rename2, add2, print)
  fork: Fork(chain1, chain2, copy_data=True)

processor: Chain(print, select, fork)
```

注意：`Fork`节点通常没有输出，因此在`Fork`之后无法添加其他节点。

