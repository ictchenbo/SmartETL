"""
Docstring for smartetl.gestata.github

Github相关操作算子

"""

import re

repo_pattern = re.compile(
    r'^https?://github\.com/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+)(?:/?$|\?|#)'
)


def code_url(repo_url: str) -> str:
    """获取仓库代码包（zip）或单文件下载url，用于下载全库代码（zip包）或指定的单个文件"""
    if "/archive/refs/heads/" in repo_url or "https://raw.githubusercontent.com/" in repo_url:
        return repo_url
    if "https://github.com/" in repo_url and "/blob/" in repo_url:
        pattern = r'^https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)$'
        match = re.match(pattern, repo_url)
        if match:
            user, repo, branch, filepath = match.groups()
            return f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{filepath}"

    m = repo_pattern.search(repo_url)
    if m:
        user, repo = m.groups()
        return f'https://github.com/{user}/{repo}/archive/refs/heads/master.zip'
    
    return None


def code_filename(repo_url: str) -> str:
    """获取保存的文件名 如果是仓库地址返回code.zip；否则如果为单个文件，返回基于该文件名后缀的文件名，如code.py/code.java；否则返回code """
    if "/archive/refs/heads/" in repo_url:
        return "code.zip"
    pattern = r'^https?://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)$'
    match = re.match(pattern, repo_url)
    if match:
        filepath = match.groups[3]
        filename = filepath[filepath.rfind('/')+1:] if '/' in filepath else filepath
        suffix = filename[filename.rfind('.'):]
        return f'code{suffix}'
    
    return 'code'
