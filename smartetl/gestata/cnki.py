from io import StringIO


def from_text(text: str):
    text_in = StringIO(text)
    buffer = {}
    for line in text_in:
        line = line.strip()
        if not line:
            if buffer:
                yield dict(buffer)
                buffer.clear()
        else:
            pos = line.find(':')
            key = line[:pos].strip()
            value = line[pos+1:].strip()
            buffer[key.split('-')[0].strip()] = value

    if buffer:
        yield buffer
