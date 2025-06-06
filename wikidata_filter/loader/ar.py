"""
多文件打包压缩文件加载器，主要包括zip/tar/7z/rar。
说明1：tar本身只打包不压缩，需配合gz/bz2/xz进行压缩
说明2：gz/bz2/xz为单文件/文件流压缩，通常与tar配合使用，未纳入本模块。通过`util.files.open`处理
"""
import traceback
from typing import Iterable, Any
from .directory import Directory


class Archive(Directory):
    """读取压缩文件 对指定后缀的文件按照默认参数进行读取 返回 (filename, data_row)"""
    def __init__(self, path: str, *suffix, type_mapping: dict = None, password: str = None, **kwargs):
        super().__init__(path, *suffix, type_mapping=type_mapping, **kwargs)
        self.password = password.encode('utf8') if password else None

    def generate(self, filename, file_stream):
        print("processing", filename)
        filetype = self.get_filetype(filename)
        cls = self.mk_builder(filetype)
        try:
            for row in cls(file_stream, **self.extra_args)():
                yield {
                    "filename": filename,
                    "data": row
                }
        except:
            print("Error occur when opening file:", filename)
            traceback.print_exc()


class Zip(Archive):
    """zip文件加载器"""
    def iter(self) -> Iterable[Any]:
        from zipfile import ZipFile
        for one in self.path:
            with ZipFile(one) as zipObj:
                for file_item in zipObj.filelist:
                    filename = file_item.filename
                    if not self.match_file(filename):
                        continue
                    with zipObj.open(filename, pwd=self.password) as f:
                        yield from self.generate(filename, f)


class Rar(Archive):
    """rar文件加载器"""
    def iter(self) -> Iterable[Any]:
        import rarfile
        for one in self.path:
            rf = rarfile.RarFile(one)
            file_list = rf.namelist()
            for filename in file_list:
                if not self.match_file(filename):
                    continue
                with rf.open(filename) as f:
                    yield from self.generate(filename, f)


class Tar(Archive):
    """tar文件加载器，支持.tar .tar.gz .tar.bz2"""
    def iter(self) -> Iterable[Any]:
        import tarfile
        for one in self.path:
            mode = 'r'
            if one.endswith('.gz'):
                mode = 'r:gz'
            elif one.endswith('.bz2'):
                mode = 'r:bz2'
            elif one.endswith('.xz'):
                mode = 'r:xz'
            with tarfile.open(one, mode) as tar:
                for filename in tar.getnames():
                    if not self.match_file(filename):
                        continue
                    with tar.open(filename) as f:
                        yield from self.generate(filename, f)


class SevenZip(Archive):
    """7z文件加载器"""
    def iter(self) -> Iterable[Any]:
        import py7zr
        for one in self.path:
            with py7zr.SevenZipFile(one) as z:
                for filename in z.namelist():
                    if not self.match_file(filename):
                        continue
                    with z.open(filename) as f:
                        yield from self.generate(filename, f)
