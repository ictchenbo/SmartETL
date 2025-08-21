import zhconv

zh_escape = {
    '鍾': '锺',
    '昇': '昇',
    '乾': '乾',
    '馀': '馀',
    '叡': '叡',
    '崑': '崑'
}


def zh_simple(s: str):
    return ''.join([zh_escape[c] if c in zh_escape else zhconv.convert(c, 'zh-hans') for c in s])
