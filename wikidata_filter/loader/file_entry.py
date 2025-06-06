"""
复杂文件类型注册在这里，根据需要动态加载对应模块
"""

ALL_LOADERS = {
    '.raw': 'file.BinaryFile',
    '.doc': 'doc.Doc',
    '.docx': 'doc.Docx',
    '.ppt': 'doc.PPT',
    '.pptx': 'doc.PPTX',
    '.pdf': 'pdf.PDF',
    '.xls': 'xls.ExcelStream',
    '.xlsx': 'xls.ExcelStream',
    '.parquet': 'parquet.Parquet',
    '.eml': 'eml.EML',
    '.pst': 'pst.PST',
    '.ost': 'pst.OST',
    '.zip': 'ar.Zip',
    '.rar': 'ar.Rar',
    '.tar': 'ar.Tar',
    '.7z': 'ar.SevenZip'
}
