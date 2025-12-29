"""

Python代码相关操作算子

"""
import sys
import ast
from typing import Set, List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ArgInfo:
    name: str   # 参数名称 str，包括self/cls等类方法特殊变量
    type: str = None   # 默认值 str表示的值
    default: str = None   # 类型注解 值为源码字符串


@dataclass
class FunctionInfo:
    name: str
    module: str
    full_name: str
    docstring: Optional[str]
    args: List[ArgInfo]
    vararg: str  # *arg
    kwarg: str   # **kwargs
    return_annotation: Optional[str]
    is_async: bool


class FunctionAnalyzer(ast.NodeVisitor):
    def __init__(self, filepath: str, prefix: str = None):
        self.filepath = filepath
        self.prefix = prefix
        self.functions: List[FunctionInfo] = []
        self._class_stack: List[ast.ClassDef] = []  # 用于判断是否在类中

    def _get_source_segment(self, node: ast.AST) -> str:
        """尝试获取节点原始源码片段（fallback 为 ast.unparse）"""
        try:
            if hasattr(ast, 'unparse'):
                return ast.unparse(node)
            else:
                # Python <3.9 fallback
                import astor
                return astor.to_source(node).strip()
        except Exception:
            return "<unavailable>"

    def visit_ClassDef(self, node: ast.ClassDef):
        pass
        # self._class_stack.append(node)
        # self.generic_visit(node)
        # self._class_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._process_function(node, is_async=False)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._process_function(node, is_async=True)
        self.generic_visit(node)
    
    def _process_function(self, node: ast.FunctionDef, is_async: bool):
        func = self.process_function(node, is_async)
        if func:
            self.functions.append(func)

    def process_function(self, node: ast.FunctionDef, is_async: bool):
        # 1. 基础信息
        name = node.name
        line = node.lineno
        is_method = len(self._class_stack) > 0

        mod = self.filepath.replace('\\', '.').replace('/', '.').replace('.py', '')
        if self.prefix and not mod.startswith(self.prefix):
            return None

        # 2. Docstring
        docstring = ast.get_docstring(node)  # 自动跳过第一个 stmt（通常是 docstring）

        # 所有参数：posonlyargs + args + kwonlyargs + vararg + kwarg
        all_args = (
            node.args.posonlyargs +
            node.args.args +
            node.args.kwonlyargs
        )

        # 默认值：仅适用于 args 和 kwonlyargs 末尾部分
        # args 默认值对应 node.args.defaults（长度 len(args) - len(defaults) 无默认）
        # kwonlyargs 默认值对应 node.args.kw_defaults
        arg_defaults = [None] * (len(node.args.args) - len(node.args.defaults)) + node.args.defaults
        # 拼接 defaults 列表（与 all_args 对齐）
        full_defaults = [None] * len(node.args.posonlyargs) + arg_defaults + node.args.kw_defaults

        # 4. 返回类型
        return_anno = self._get_source_segment(node.returns) if node.returns else None
        
        args = []
        for arg_node, def_node in zip(all_args, full_defaults):
            _type = None
            _default = None
            if def_node is not None:
                _default = self._get_source_segment(def_node)
            if arg_node.annotation:
                _type = self._get_source_segment(arg_node.annotation)
            args.append(ArgInfo(name=arg_node.arg, type=_type, default=_default))

        fullname = f'{mod}.{name}'

        # 构建 FunctionInfo
        func_info = FunctionInfo(
            name=name,
            module=mod,
            full_name=fullname,
            docstring=docstring,
            args=args,
            vararg=node.args.vararg.arg if node.args.vararg else None,
            kwarg=node.args.kwarg.arg if node.args.kwarg else None,
            return_annotation=return_anno,
            is_async=is_async
        )
        return func_info


@dataclass
class ClassInfo:
    name: str
    bases: List[str]
    module: str
    full_name: str
    short_name: str
    docstring: str
    init: FunctionInfo




class ClassAnalyzer(ast.NodeVisitor):
    def __init__(self, current_file: str, prefix: str):
        """
        prefix: 类所在模块的前缀，如 'loader', 'processor'
        """
        self.prefix = prefix
        self.matching_classes: List[ClassInfo] = [] 
        self._current_file: str = current_file

    def visit_ClassDef(self, node: ast.ClassDef):
        class_name = node.name
        # line = node.lineno
        # print('class ', self._current_file, class_name, node.bases)

        mod = self._current_file.replace('\\', '.').replace('/', '.').replace('.py', '')
        if not mod.startswith(self.prefix):
            self.generic_visit(node)
            return

        fullname = f'{mod}.{class_name}'
        shortname = fullname.replace(self.prefix + '.', '')

        # 解析所有基类（支持多继承）
        base_refs = []
        for base in node.bases:
            base_ref = self._resolve_base_name(base)
            base_refs.append(base_ref)
        
        # 解析__init__方法
        init_func = None
        for stmt in node.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)) and stmt.name == "__init__":
                func_analyzer = FunctionAnalyzer(self._current_file)
                init_func = func_analyzer.process_function(stmt, isinstance(stmt, ast.AsyncFunctionDef))
                break

        # 获取类docstring
        docstring = ast.get_docstring(node)

        class_info = ClassInfo(
            name=class_name, 
            bases=base_refs, 
            module=mod, 
            full_name=fullname, 
            short_name=shortname,
            docstring=docstring,
            init=init_func
        )

        self.matching_classes.append(class_info)

        self.generic_visit(node)

    def _resolve_base_name(self, node: ast.AST) -> str:
        """
        将 AST 节点转为可读的基类名字符串，支持：
          - Name('ABC') → 'ABC'
          - Attribute(Name('abc'), 'ABC') → 'abc.ABC'
          - Call(Name('dict'), ...) → 'dict' （忽略调用参数）
        """
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # abc.ABC → 'abc.ABC'
            prefix = self._resolve_base_name(node.value)
            return f"{prefix}.{node.attr}"
        elif isinstance(node, ast.Call):
            # dict(...) 继承 → 只取函数名
            return self._resolve_base_name(node.func)
        elif isinstance(node, ast.Subscript):
            # Generic[T] → 取主类名 Generic
            return self._resolve_base_name(node.value)
        else:
            # 兜底：ast.unparse（Python ≥3.9）
            try:
                if hasattr(ast, 'unparse'):
                    return ast.unparse(node)
                else:
                    import astor
                    return astor.to_source(node).strip()
            except Exception:
                return "<unknown>"


def normal(filepath: str):
    """判断是否为正常的python代码文件"""
    py_file = Path(filepath)
    for part in py_file.parts:
        if part.startswith((".", "_")) or part in ("venv", "env", ".git", "__pycache__" ):
            return False
    
    return True


def build_ast(py_file: str):
    """构建Python文件ast结构"""
    try:
        with open(py_file, "r", encoding="utf-8") as f:
            return ast.parse(f.read(), filename=py_file)
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"⚠️ Skip {py_file}: {e}", file=sys.stderr)
    
    return None


def analyze_classes(tree, py_file: str, prefix: str = None) -> List[ClassInfo]:
    """分析提取类信息"""
    analyzer = ClassAnalyzer(py_file, prefix)
    analyzer.visit(tree)
    return analyzer.matching_classes


def analyze_functions(tree, py_file: str, prefix: str = None) -> List[FunctionInfo]:
    """分析提取函数信息"""
    analyzer = FunctionAnalyzer(py_file, prefix)
    analyzer.visit(tree)
    return analyzer.functions
