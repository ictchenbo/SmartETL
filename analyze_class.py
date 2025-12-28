# analyze_inheritance.py
import ast
import sys
from pathlib import Path
from typing import Set, List, Dict, Tuple, Optional
from dataclasses import dataclass, asdict

from analyze_function import FunctionInfo, FunctionAnalyzer


@dataclass
class ClassInfo:
    name: str
    bases: List[str]
    module: str
    full_name: str
    short_name: str
    docstring: str
    init: FunctionInfo


class InheritanceAnalyzer(ast.NodeVisitor):
    def __init__(self, prefix):
        """
        prefix: 类所在模块的前缀，如 'loader', 'processor'
        """
        self.prefix = prefix
        self.matching_classes: List[ClassInfo] = [] 
        self._current_file: str = ''

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


def find_subclasses(project_dir: str, prefix: str) -> List[ClassInfo]:
    """
    扫描项目，返回类信息
    """
    project_path = Path(project_dir)

    analyzer = InheritanceAnalyzer(prefix)

    for py_file in project_path.rglob("*.py"):
        # 忽略虚拟环境、隐藏文件等
        if any(part.startswith((".", "_")) or part in ("venv", "env", ".git", "__pycache__")
               for part in py_file.parts):
            continue

        # print("visiting", py_file)

        try:
            with open(py_file, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(py_file))
        except (SyntaxError, UnicodeDecodeError) as e:
            print(f"⚠️ Skip {py_file}: {e}", file=sys.stderr)
            continue

        analyzer._current_file = str(py_file)
        analyzer.visit(tree)

    return analyzer.matching_classes


def export_to_json(items: List[ClassInfo], output_file: str = "class.json"):
    import json
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([asdict(item) for item in items], f, indent=2, ensure_ascii=False)
    print(f"📤 Exported {len(items)} classes to {output_file}")


# ===== CLI 使用示例 =====
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Find classes inheriting from a given base class.")
    parser.add_argument("--output", "-o", default=None, help="Output JSON file (e.g., class.json)")
    parser.add_argument("project", help="Project root directory")
    parser.add_argument("prefix", help="class module prefix to search for (e.g., 'loader', 'processor')")
    
    args = parser.parse_args()

    subclasses = find_subclasses(args.project, args.prefix)

    print(f"🔍 Found {len(subclasses)} class(es):\n")
    for info in subclasses:
        # rel_file = Path(file).resolve().relative_to(args.project)
        print(f"  ✅ {info}")

    if not subclasses:
        print("  ❌ None found.")
    elif args.output:
        export_to_json(subclasses, args.output)


if __name__ == "__main__":
    main()
