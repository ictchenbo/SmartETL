from datetime import datetime
from wikidata_filter.loader.file import File

DATETIME_FORMAT = '%Y-%m-%d %H:%M'


class Excel(File):
    """
    Excel文件内容加载 基于pandas 适合小文件
    """
    def __init__(self, input_file: str, sheets: list = None, with_header: bool = True):
        self.input_file = input_file
        self.sheets = sheets
        self.header = with_header

        try:
            import pandas
        except ImportError:
            raise Exception("failed to import pandas needed by Excel parsing")

        self.pandas = pandas
        self.xl = pandas.ExcelFile(input_file)

    def iter(self):
        sheet_names = self.xl.sheet_names
        if self.sheets:
            sheet_names = [i if isinstance(i, str) else sheet_names[i] for i in self.sheets]

        for sheet in sheet_names:
            current_sheet = self.xl.parse(sheet, header=None)

            # 获取最大行和最大列数
            nrows = current_sheet.shape[0]
            ncols = current_sheet.columns.size

            if nrows <= 0 or ncols <= 0:
                continue

            header = None

            for iRow in range(nrows):
                row = []
                for iCol in range(ncols):
                    item = current_sheet.iloc[iRow, iCol]
                    if self.pandas.isna(item) or not item:
                        item = ""
                    elif isinstance(item, self.pandas.Timestamp):
                        item = item.strftime(DATETIME_FORMAT)
                    row.append(item)

                if self.header:
                    if iRow == 0:
                        header = row
                    else:
                        output = dict(zip(header, row))
                        output['_sheet'] = sheet
                        yield output
                else:
                    yield sheet, row


class ExcelStream(File):
    """Excel流式读取，适合大文件"""
    def __init__(self, input_file: str, sheets: list = None, with_header: bool = True):
        self.input_file = input_file
        self.sheets = sheets
        self.with_header = with_header

        try:
            import openpyxl
        except ImportError:
            raise ImportError("failed to import openpyxl  needed by Excel parsing")

        self.instream = openpyxl.load_workbook(self.input_file, read_only=True)

        sheet_names = self.instream.sheetnames
        if self.sheets:
            sheet_names = [i if isinstance(i, str) else sheet_names[i] for i in self.sheets]
        # print("sheet_names", sheet_names)
        self.sheet_names = sheet_names

    def parse_row(self, row: tuple):
        vals = []
        for v in row:
            if isinstance(v, datetime):
                vals.append(v.strftime(DATETIME_FORMAT))
            else:
                vals.append(v)

        return vals

    def iter(self):
        for sheet_name in self.sheet_names:
            sheet = self.instream.get_sheet_by_name(sheet_name)

            header = None
            for row in sheet.iter_rows(values_only=True):
                row_values = self.parse_row(row)
                if self.with_header:
                    if header is None:
                        header = row_values
                    else:
                        output = dict(zip(header, row_values))
                        output['_sheet'] = sheet_name
                        yield output
                else:
                    yield sheet_name, row_values
