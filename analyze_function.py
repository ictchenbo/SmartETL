# analyze_functions.py
import os
import ast
import sys
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Optional, Any, Dict


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
    def __init__(self, filepath: str):
        self.filepath = filepath
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
        self.functions.append(func)

    def process_function(self, node: ast.FunctionDef, is_async: bool):
        # 1. 基础信息
        name = node.name
        line = node.lineno
        is_method = len(self._class_stack) > 0

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

        mod = self.filepath.replace('\\', '.').replace('/', '.').replace('.py', '')
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


def collect_all_functions(project_dir: str) -> List[FunctionInfo]:
    project_path = Path(project_dir)
    if not project_path.is_dir():
        raise ValueError(f"Directory not found: {project_dir}")

    all_funcs: List[FunctionInfo] = []

    for py_file in project_path.rglob("*.py"):
        if any(part.startswith((".", "_")) or part in ("venv", "env", "build", "dist") 
               for part in py_file.parts):
            continue  # 跳过隐藏目录、虚拟环境等

        try:
            with open(py_file, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(py_file))
        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"⚠️ Skip {py_file}: {e}", file=sys.stderr)
            continue

        analyzer = FunctionAnalyzer(str(py_file))
        analyzer.visit(tree)
        all_funcs.extend(analyzer.functions)

    return all_funcs


def print_summary(functions: List[FunctionInfo]):
    print(f"✅ Collected {len(functions)} functions/methods.\n")

    for i, f in enumerate(functions[:10], 1):  # 仅展示前10个示例
        kind = "async def" if f.is_async else "def"
        print(f"[{i}] {kind} {f.name}(...)  [function in {f.module}]")
        print(f"    Args: {f.args}")
        if f.return_annotation:
            print(f"    → Returns: {f.return_annotation}")
        if f.docstring:
            doc = f.docstring.strip().split('\n')[0][:60] + ("..." if len(f.docstring) > 60 else "")
            print(f"    Doc: \"{doc}\"")
        print()

    if len(functions) > 10:
        print(f"... and {len(functions) - 10} more.")


def export_to_json(functions: List[FunctionInfo], output_file: str = "functions.json"):
    import json
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([asdict(func) for func in functions], f, indent=2, ensure_ascii=False)
    print(f"📤 Exported {len(functions)} functions to {output_file}")


# ===== CLI 入口 =====
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extract function signatures & docstrings via AST.")
    parser.add_argument("project_path", help="Root directory of Python project")
    parser.add_argument("--output", "-o", default=None, help="Output JSON file (e.g., funcs.json)")
    parser.add_argument("--limit", type=int, default=5, help="Max functions to preview (default: 5)")
    args = parser.parse_args()

    funcs = collect_all_functions(args.project_path)

    print_summary(funcs[:args.limit])

    if args.output:
        export_to_json(funcs, args.output)


if __name__ == "__main__":
    main()
