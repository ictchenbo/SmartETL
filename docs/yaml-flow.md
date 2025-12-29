## Yaml Flow设计说明
基于YAML文件格式定义的数据处理流程

### 文件结构
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
11. `limit: int` 处理数据限制 可用于数据预览、流程开发调试


总的来说，本框架实现的就是从`loader`加载数据 并通过`processor`进行处理

### from指令
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

### consts指令
功能：定义可用于流程节点的常量数据。在节点参数复杂情况下非常有用。

`consts`支持所有yaml数据类型，包括数值、字符串、字典、数组等。具体包括：
1. 常量，直接定义变量值
2. 计算值，形如`eval(expr)` 对expr定义的python表达式进行计算获取值。基于这种方式还可以获取命令行参数 如arg1 arg2
3. 环境变量，形如`$env`，即获取环境变量中定义的env变量值

### nodes指令
功能：以字典（key-value，str->str）形式定义流程的计算节点或数据节点。

支持定义3种类型节点：
1. 普通节点/对象节点：通过类构造函数调用生成的对象节点，主要用于数据处理。例如：`print: Print` 定义了一个名为`print`的打印节点。这是流程定义中最主要的节点。
2. 计算节点：提供函数调用表达式，将函数调用结果作为当前节点的值。常用于配置文件读取、字典加载等。例如：`types: util.sets.from_csv('config/event_type.txt')` 定义了一个名为`types`的数据节点，其值为`from_csv`函数调用结果
3. 表达式节点：形如`=expr`，`expr`表达式执行结果作为当前节点的值。常用于数据结构动态定义、Lambda函数定义。例如`is_html: "=lambda r: r['filename'].endswith('.html')"` 定义了一个名为`is_html`的表达式节点，其值Lambda函数，判断filename字段是否以.html结尾（即通过文件名后缀判断是否为html文件）

本质上，3种类型节点的定义都是通过对value表达式（有效的Python表达式）的执行。其中，普通节点通过执行对应类的构造方法（`__init__`），获取对象作为节点的值，在流程处理中调用该对象的`__process__`方法，实现数据处理。计算节点是直接调用函数，将调用结果作为节点的值。表达式节点是执行表达式，获取结果（通常为一个Lambda函数）作为节点的值。

上述三种节点都可以引用已经定义的节点，以及命令行参数（arg1，arg2，__var等）和常量区`consts`。

普通节点和计算节点的定义，需要准确引用其使用的组件。

### 组件引用
组件引用名称分为短名和长名。短名形如"Comp"，例如`Print` 或者 `Count(label='test')`。长名形如“mod.Comp”，例如`ar.Zip`或`util.files.text`。

`nodes`指令中的节点类型，根据不同组件类型，组件定位规则如下：
1. 如果key为loaderX/processorX/databaseX，则表示在对应类型包（loader/process/database）中查找组件，无论短名还是长名；
2. 否则，如果引用名称为长名且顶级包有效（如`util` `database` `gestata`），则在对应包中查找；（`util.database.X` -> `database.X` 以兼容V2）
3. 否则，在processor包中查找，无论短名还是长名；

`loader`指令默认在loader包中定位。`processor`指令默认在processor包中定位。

### 节点构造
构造器形式：
- 短名：`<Comp>(arg1, arg2, *args, k1=v1, k2=v2, ..., **kwargs)` ()为节点构造参数，如果参数为空，可以省略不写，如`Print()` 等价于 `Print`
- 长名： `<module>.<Comp>(*args, **kwargs)` ()为节点构造参数，如果参数为空，可以省略不写

构造器支持嵌套，例如`Chain(Print())`，但注意，嵌套构造器仅支持使用短名，且括号()不能省略。

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

