import os
import subprocess

from typing import Iterable, Any
from smartetl.loader.file import BinaryFile


class PST(BinaryFile):
    """使用readpst将PST文件解压到临时文件夹 返回每封邮件"""
    def __init__(self, input_file: str, tmp_dir: str = None, **kwargs):
        super().__init__(input_file, auto_open=False)
        self.tmp_dir = tmp_dir or '.tmp'
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)

    def pst2eml(self):
        return subprocess.call(['readpst', '-D', '-S', '-M', '-b', '-j', '20', '-o', self.tmp_dir, self.filename])

    def iter(self) -> Iterable[Any]:
        file_list = os.listdir(self.tmp_dir)
        for dir_name in file_list:
            sub_dir = os.path.join(self.filename, dir_name)
            if os.path.isdir(sub_dir):
                for root, dirs, files in os.walk(sub_dir):
                    for name in files:
                        yield os.path.join(root, name)
            subprocess.call(['rm', '-rf', sub_dir])


class OST(BinaryFile):
    """使用readpst将OST文件解压到临时文件夹 返回每封邮件"""
    def __init__(self, input_file: str, tmp_dir: str = None, **kwargs):
        super().__init__(input_file, auto_open=False)
        self.tmp_dir = tmp_dir or '.tmp'
        if not os.path.exists(tmp_dir):
            os.mkdir(tmp_dir)

    def ost2eml(self):
        return subprocess.call(['readpst', '-M', '-o', self.tmp_dir, self.filename])

    def iter(self) -> Iterable[Any]:
        file_list = os.listdir(self.tmp_dir)
        for dir_name in file_list:
            sub_dir = os.path.join(self.filename, dir_name)
            if os.path.isdir(sub_dir):
                for root, dirs, files in os.walk(sub_dir):
                    for name in files:
                        # 在不影响其他文件夹的情况下跳过 '日历' 和 '同步问题' 文件夹
                        yield os.path.join(root, name)
                    # aa = root.split(dir_name + '\\')
                    # if len(aa) >= 2:
                    #     tem = root.split(dir_name + '\\')[1]
                    #     if len(tem) >= 2 and tem[:2] == '日历':
                    #         continue
                    #     if len(tem) >= 4 and tem[:4] == '同步问题':
                    #         continue
            subprocess.call(['rm', '-rf', sub_dir])
