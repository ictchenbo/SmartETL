## Yaml Flow
基于YAML文件格式定义的数据处理流程

### 字段说明
1. `name: str` 流程名称
2. `version: str` 流程版本号
3. `author: str` 作者
4. `description: str` 流程描述
5. `arguments: int` 流程接受的运行时参数个数 如果提供参数不足将报错。通过arg1,arg2,...进行引用
6. `consts` 可用于组件的常量数据 支持通过$<VAR> 引用环境变量
7. `nodes` 处理节点组件（包括动态变量定义 后定义的变量可引用前面定义的变量） 支持python表达式
8. `loader` 数据加载器组件，可引用`nodes`中已定义节点或创建新的节点
9. `processor` 数据处理器组件，通过引用`nodes`中变量定义主流程
10. `from` 集成的其他流程定义，支持单个文件或一组文件，如果文件不存在或出现循环引用将报错


总的来说，本框架实现的就是从`loader`加载数据 并通过`processor`进行处理
