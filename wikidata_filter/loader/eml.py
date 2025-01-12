import os
import time
from datetime import datetime
import re
import traceback
import uuid

from typing import Iterable, Any
from wikidata_filter.loader.file import BinaryFile
from wikidata_filter.util.html_util import text_from_html

import email
import email.utils
import email.header


def change(raw_subject):
    decoded_header = email.header.decode_header(raw_subject)
    decoded_subject, encoding = decoded_header[0]
    if isinstance(decoded_subject, bytes):
        decoded_subject = decoded_subject.decode(encoding or 'utf-8')
    return decoded_subject


def to_timestamp(date_time_str):
    """解析邮件头的日期"""
    try:
        dt = email.utils.parsedate_to_datetime(date_time_str)
    except TypeError:
        # 存在"22/12/2006 4:19:08 PM"的时间格式，需要单独处理下
        try:
            date_format = "%d/%m/%Y %I:%M:%S %p"
            dt = datetime.strptime(date_time_str, date_format)
        except Exception:
            return None
    except ValueError:
        # 非标准日期格式，无法识别转换
        return None

    timestamp_seconds = int(time.mktime(dt.timetuple()))
    timestamp_milliseconds = timestamp_seconds * 1000
    return timestamp_milliseconds


def remove_special_chars(s: str):
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    if s.startswith("'") and s.endswith("'"):
        s = s[1:-1]

    return re.sub(r'[\n\r]+', '', s)


def parse_addr_list(header: str):
    """解析邮件头的地址列表"""
    if not header:
        return []
    print(header)
    ret = []
    for addr in re.split(r'[\n\r;]+', header):
        name = ''
        pos = addr.find('<')
        if pos > 0:
            name = change(addr[:pos]).strip()
            addr = addr[pos+1:-1]
        addr = remove_special_chars(addr).strip()
        if not addr:
            continue
        if not name:
            name = addr.split('@')[0]
        ret.append({
            "n": name,
            "a": addr
        })
    return ret


def parse_attachment(part):
    """解析附件信息"""
    name = part.get_param("name")
    if not name:
        return None
    fname = email.header.decode_header(name)[0]
    if fname[1]:
        attr_name = fname[0].decode(fname[1])
    else:
        attr_name = fname[0]

    attr_data = part.get_payload(decode=True)

    return {
        "name": attr_name,
        "type": attr_name.split(".")[-1],
        "size": len(attr_data),
        "content": attr_data
    }


def parse_body(path: str, default_encoding='utf-8'):
    """解析邮件的body部分 邮件通常包含1个或2个正文（分别为text/plain、text/html）以及若干个附件部分"""
    parts = {}
    attachments = []
    with open(path, 'r', encoding="ISO-8859-1") as f:  # pst解析出的eml只能ISO-8859-1编码
        try:
            msg = email.message_from_file(f)
        except UnicodeDecodeError:
            print(traceback.format_exc())
            return None, []
        for part in msg.walk():
            mime_type = part.get_content_type()
            if mime_type in ('text/plain', 'text/html'):
                is_char = part.get_content_charset() or default_encoding
                try:
                    text = part.get_payload(decode=True).decode(is_char.lower())
                except Exception:
                    text = part.get_payload(decode=True).decode(is_char.lower(), errors="ignore")
                parts[mime_type] = text
            elif not part.is_multipart():
                att = parse_attachment(part)
                if att:
                    attachments.append(att)

    return parts, attachments


def parse_body2(path: str):
    msg = email.message_from_binary_file(open(path, "rb"), policy=email.policy.default)
    charset = None
    body = ""

    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))

            # skip any text/plain (txt) attachments
            if ctype == 'text/plain' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)  # decode
                charset = part.get_charset()
                break
    # not multipart - i.e. plain text, no attachments, keeping fingers crossed
    else:
        body = msg.get_payload(decode=True)
        charset = msg.get_charset()

    body = body.decode(charset or 'utf8')
    return body


def write_attachment(att, save_dir: str):
    """保存附件"""
    data = att.pop('content')
    filename = f'{att["size"]}_{att["name"]}'
    fp = os.path.join(save_dir, filename)
    with open(fp, 'wb') as f_write:
        f_write.write(data)
    return filename


skip_headers = ['content-type']


def parse_header(path: str):
    with open(path, 'r', encoding="utf-8") as file:
        eml_content = file.read()
        msg = email.message_from_string(eml_content)

        row = {
            "subject": remove_special_chars(change(msg['Subject'])),
            "date": to_timestamp(change(msg['Date'])),
            "from": parse_addr_list(msg.get("From")),
            "to": parse_addr_list(msg.get("To")),
            "cc": parse_addr_list(msg.get("Cc")),
            "bcc": parse_addr_list(msg.get("Bcc"))
        }
        ext = []
        for k, v in msg.raw_items():
            if k.lower() not in row and k.lower() not in skip_headers:
                ext.append({
                    "key": k,
                    "value": remove_special_chars(change(v))
                })
        row['ext_headers'] = ext

        return row


def __parse_header__(path):
    headers = {
        "From": None,
        "To": [],
        "Cc": [],
        "Bcc": [],
        "Date": None,
        "Subject": None,
        "Message-ID": None
    }

    with open(path, 'r', encoding="utf-8", errors="ignore") as file:
        for line in file:
            if line.startswith("From") and not headers["From"]:
                headers["From"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""
            elif line.startswith("To") and not headers["To"]:
                to_emails = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""
                headers["To"].append(to_emails.strip())
            elif line.startswith("Date") and not headers["Date"]:
                headers["Date"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else 0
            elif line.startswith("Subject") and not headers["Subject"]:
                headers["Subject"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""
            elif line.startswith("Message-ID") and not headers["Message-ID"]:
                headers["Message-ID"] = line.split(":")[1].strip() if len(line.split(":")) > 1 else ""
    for field in ("To", "Cc", "Bcc"):
        # todo: 目前先抽取一个
        if headers[field]:
            headers[field] = headers[field][0]
    return headers


class EML(BinaryFile):
    """解析EML格式邮件 产生邮件正文"""
    def __init__(self, input_file: str, tmp_dir: str = None, save_attachment: bool = True, **kwargs):
        super().__init__(input_file, auto_open=False)
        self.tmp_dir = tmp_dir or '.tmp'
        self.save_attachment = save_attachment
        if save_attachment and not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def iter(self) -> Iterable[Any]:
        eml = parse_header(self.filename)
        parts, atts = parse_body(self.filename)
        if self.save_attachment:
            for att in atts:
                att['filepath'] = write_attachment(att, self.tmp_dir)

        if parts is None:
            # 解析错误
            eml['__error'] = True

        if 'text/html' in parts:
            eml['html'] = parts['text/html']
        if 'text/plain' in parts:
            eml['text'] = parts['text/plain']
        elif 'html' in eml:
            eml['text'] = text_from_html(eml['html'])

        if not eml.get('text'):
            print('Warning: The email body is empty!')
            eml['__empty'] = True

        eml['atts'] = atts

        yield eml
