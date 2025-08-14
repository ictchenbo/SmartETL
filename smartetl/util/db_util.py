INT8 = 'Int8'
INT16 = 'Int16'
INT32 = 'Int32'
INT64 = 'Int64'
UINT8 = 'UInt8'
UInt16 = 'UInt16'
UInt32 = 'UInt32'
UInt64 = 'UInt64'
FLOAT8 = 'Float8'
FLOAT16 = 'Float16'
FLOAT32 = 'Float32'
FLOAT64 = 'Float64'
STRING = 'String'
DATE = 'Date'
DATETIME = 'DateTime'

common_types = {
    'int': INT32,
    'bigint': INT64,
    'float': FLOAT32,
    'double': FLOAT64,
    'text': STRING,
    'varchar': STRING,
    'char': STRING,
    'date': DATE,
    'datetime': DATETIME
}


def dtype_bool(_val):
    return _val == 'YES' or _val is True or _val != 0


def dtype_mysql(_type: str):
    if _type.startswith('varchar') or _type.startswith('char'):
        return STRING
    if _type.startswith('tinyint'):
        return INT8

    _type_s = _type.lower()
    if _type_s in common_types:
        return common_types[_type_s]

    return _type


