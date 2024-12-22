"""
利用pandas读取多种文件

@dependencies pandas
"""

from wikidata_filter.loader.base import DataProvider

try:
    import pandas
except:
    print("Error! you need to install pandas first: pip install pandas")
    raise ImportError("pandas not installed")

DATETIME_FORMAT = '%Y-%m-%d %H:%M'


class Excel(DataProvider):
    """
    Excel文件内容加载
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
