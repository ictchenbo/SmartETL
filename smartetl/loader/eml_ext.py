import email
import chardet
import os
import re
import base64
import quopri
import uuid
import binascii
from email.header import decode_header
# 确实为单一部份邮件: 该情况下，split单一片段没有Content-Type, 默认为text/plain

update_tmp_folder = "/app/update_tmp"


def mk_id():
    return str(uuid.uuid1()).replace('-', '')


def is_base64(s):
    try:
        base64.b64decode(s).decode('utf-8', errors = "ignore")
        return True
    except (binascii.Error, UnicodeDecodeError):
        return False


def parser_annex_filename(raw):
    decoded_str = ""
    decoded_parts = decode_header(raw)
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            if encoding is None:
                encoding = "utf-8"
            decoded_str += part.decode(encoding, errors="ignore")
        else:
            decoded_str += part
    return decoded_str


def parse_single__(msg, parts: list, payload, file_path: str):
    # 确实为单一部分邮件
    print("#################flag：确实为单一部份邮件！")
    content_type = msg.get_content_type()
    charset = msg.get_content_charset()
    mgp = msg.get_payload()
    char_matches = re.search(r'charset=(.*)', mgp)
    char_type = char_matches.group(1) if char_matches else charset
    if char_type is None:
        detected_charset = chardet.detect(payload)['encoding']
        if detected_charset:
            char_type = detected_charset

    is_continue = True
    try:
        decoded_payload = payload.decode(char_type, errors="replace") if char_type else payload.decode(
            'utf-8', errors='replace')
    except LookupError as e:
        if "unknown encoding" in str(e):
            decoded_payload = payload.decode("utf-8", errors="ignore")
        else:
            print(f"无法校验该文本的编码，pass，所属路径为: {file_path}, 错误信息为：{str(e)}")
    dps = [_ for _ in decoded_payload.split("\n\n") if _]
    if len(dps) == 1:
        dps = [""] + dps

    # 完全重复代码，需抽取
    if "Content-Transfer-Encoding: quoted-printable" in decoded_payload:
        try:
            decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode(char_type,
                                                                                        errors="ignore")
        except (UnicodeDecodeError, LookupError):
            try:
                decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode("utf-8",
                                                                                            errors="ignore")
            except UnicodeDecodeError:
                decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode("iso-8859-1",
                                                                                            errors="ignore")
        except Exception as e:
            print(f"存在未知编码格式，所属路径为：{file_path}, 错误为:{e} pass!")
            is_continue = False
    else:
        # if "Content-Transfer-Encoding: base64" in decoded_payload:
        try:
            decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode(char_type, errors="ignore")
        except (UnicodeDecodeError, LookupError):
            try:
                decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode("utf-8", errors="ignore")
            except UnicodeDecodeError:
                decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode("iso-8859-1",
                                                                                errors="ignore")
        except ValueError:
            pass
        except Exception:
            print(f"存在未知编码格式，所属路径为：{file_path}, pass!")
            is_continue = False
    if is_continue:
        if decoded_payload:
            flag = "text/plain" if "text/plain" in content_type else (
                "text/html" if "text/html" in content_type else "")
            if flag:
                parts.append({
                    "mimeType": flag,
                    "content": decoded_payload
                })


def parse_single_body(msg, parts: list, atts: list, file_path: str):
    # 非多部分邮件，但进一步检查是否有类似多部分的结构标志
    payload = msg.get_payload(decode=True)
    if not payload:
        return
    print("#################flag：非多部分邮件，但进一步检查是否有类似多部分的结构标志!")
    # 使用正则表达式在整个邮件内容中查找所有可能的边界分隔符
    boundary_pattern = re.compile(r'boundary="([^"]*)"')
    try:
        matches = boundary_pattern.findall(str(msg))
    except KeyError as e:
        raise ValueError(f"该邮件格式及其不标准，且无法通过规则手动抽取, {e}")
    boundaries = ["--" + boundary for boundary in matches]
    print(f"#################boundaries({len(boundaries)})：", boundaries)

    if not boundaries:
        parse_single__(msg, parts, payload, file_path)
        return

    print("######################flag：该邮件为单结构拆分多结构！")
    temp_payload = payload
    for boundary in boundaries:
        subparts = temp_payload.split(boundary.encode())
        t_pay_1 = b''.join(subparts[1:]) if len(subparts) > 1 else b''
        for subpart in subparts:
            i = True
            temp_msg = email.message_from_bytes(subpart)
            temp_charset = temp_msg.get_content_charset()
            temp_disposition = temp_msg.get('Content-Disposition', None)
            t_pay_2 = temp_msg.get_payload(decode=True)
            if t_pay_2 is None:
                continue
            tpd = t_pay_2.decode("utf-8", errors="ignore")
            content_type_matches = re.search(r'Content-Type:\s+(.*?);', tpd)
            temp_content_type = content_type_matches.group(
                1) if content_type_matches else temp_msg.get_content_type()

            for bd in boundaries:
                if bd[2:] in tpd:
                    i = False
            if i is False:
                continue
            if ': attachment;' in tpd:
                tfe = re.compile(r'filename="([^"]*)"')
                mas = tfe.findall(str(tpd))
                if mas:
                    attr_name = parser_annex_filename(mas[0])
                    t_split = [_ for _ in tpd.split("\n\n") if _]
                    header_data = "".join(t_split[:-1])
                    str_data = t_split[-1]

                    if "Content-Transfer-Encoding: base64" in header_data:
                        try:
                            attr_data = base64.b64decode(str_data)
                        except binascii.Error as e:  # 编码不全不标准，增加"="结尾再次尝试, 3 or 4? 需验证
                            if "Incorrect padding" in str(e):
                                missing_padding = len(str_data) % 4
                                if missing_padding:
                                    str_data += "=" * (4 - missing_padding)
                                    try:
                                        attr_data = base64.b64decode(str_data)
                                    except binascii.Error as e:
                                        print(
                                            f"存在错误解析的附件, 且加标识符无效：{e}，所属路径为：{file_path}, pass!")
                                        continue
                                else:
                                    print(f"存在错误解析的附件：{e}，所属路径为：{file_path}, pass!")
                                    continue
                    elif "Content-Transfer-Encoding: quoted-printable" in header_data:
                        try:
                            attr_data = quopri.decodestring(str_data)
                        except Exception as e:
                            print(f"存在quoted-pritable编码解析错误，所属路径为：{file_path}, pass!")
                            continue
                    else:
                        print(f"附件解析时,存在未知编码格式，所属路径为：{file_path}, pass!")
                        continue
                    att_e = attr_name.split(".")
                    attr_name = ".".join(att_e[:-1] + [re.sub(r"[^a-zA-Z0-9]", '', att_e[-1])])
                    filename = "{0}.{1}".format(mk_id(), attr_name.split(".")[-1])
                    attr_fp = os.path.join(update_tmp_folder, filename)
                    # print("----------------attr_fp--------------")
                    # print(attr_fp)
                    with open(attr_fp, 'wb') as f_write:
                        f_write.write(attr_data)
                    atts.append({
                        "filename": filename,
                        "name": attr_name,
                        "type": attr_name.split(".")[-1],
                        "size": os.path.getsize(attr_fp)
                    })
            else:
                if t_pay_2:
                    # 校验编码
                    char_matches = re.search(r'charset=(.*)', tpd)
                    char_type = char_matches.group(1) if char_matches else temp_charset
                    if char_type is None:
                        detected_charset = chardet.detect(t_pay_2)['encoding']
                        if detected_charset:
                            char_type = detected_charset
                    try:
                        try:
                            decoded_payload = t_pay_2.decode(char_type,
                                                             errors="ignore") if char_type else t_pay_2.decode(
                                'utf-8', errors='replace')
                        except LookupError as e:
                            if "unknown encoding" in str(e):
                                decoded_payload = t_pay_2.decode("utf-8", errors="ignore")
                            else:
                                print(f"无法校验该文本的编码，pass，所属路径为: {file_path}, 错误信息为：{str(e)}")
                        dps = [_ for _ in decoded_payload.split("\n\n") if _]
                        if "Content-Transfer-Encoding: base64" in decoded_payload:
                            try:
                                decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode(char_type,
                                                                                                errors="ignore")
                            except (UnicodeDecodeError, LookupError):
                                try:
                                    decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode("utf-8",
                                                                                                    errors="ignore")
                                except UnicodeDecodeError:
                                    decoded_payload = base64.b64decode("\n\n".join(dps[1:])).decode(
                                        "iso-8859-1", errors="ignore")
                            except Exception as e:
                                print(f"存在未知编码格式，所属路径为：{file_path}, 错误为:{e} pass!")
                                continue
                        elif "Content-Transfer-Encoding: quoted-printable" in decoded_payload:
                            try:
                                decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode(
                                    char_type, errors="ignore")
                            except (UnicodeDecodeError, LookupError):
                                try:
                                    decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode(
                                        "utf-8", errors="ignore")
                                except UnicodeDecodeError:
                                    decoded_payload = quopri.decodestring("\n\n".join(dps[1:]).encode()).decode(
                                        "iso-8859-1", errors="ignore")
                            except Exception as e:
                                print(f"存在未知编码格式，所属路径为：{file_path}, 错误为:{e} pass!")
                                continue
                        else:
                            print(f"存在未知编码格式，所属路径为：{file_path}, pass!")
                            continue
                        if decoded_payload:
                            flag = "text/plain" if "text/plain" in temp_content_type else (
                                "text/html" if "text/html" in temp_content_type else "")
                            if flag:
                                parts.append({
                                    "mimeType": flag,
                                    "content": decoded_payload
                                })
                    except Exception:
                        raise


def parse_multipart(part, parts: list, atts: list):
    content_type = part.get_content_type()
    charset = part.get_content_charset()
    disposition = part.get('Content-Disposition', None)

    name = part.get_param("name")
    if name:
        # 附件
        try:
            fname = decode_header(name)[0]
        except Exception as e:
            fname = ("", "")
        if fname[1]:
            try:
                attr_name = fname[0].decode(fname[1], errors="ignore")
            except LookupError:
                if fname[1] in ("136"):
                    attr_name = fname[0].decode('utf-8', errors='ignore')
                else:
                    print("存在错误的编码格式，pass！")
                return
        else:
            attr_name = fname[0]
        # print("附件名:", attr_name)
        # 解码附件内容
        if isinstance(attr_name, bytes):
            attr_name = attr_name.decode("utf-8", errors="ignore")
        att_e = attr_name.split(".")
        attr_name = ".".join(att_e[:-1] + [re.sub(r"[^a-zA-Z0-9]", '', att_e[-1])])
        filename = "{0}.{1}".format(mk_id(), attr_name.split(".")[-1])
        attr_data = part.get_payload(decode=True)
        attr_fp = os.path.join(update_tmp_folder, filename)
        try:
            with open(attr_fp, 'wb') as f_write:
                f_write.write(attr_data)
        except ValueError as e:
            if "embedded null byte" in str(e):
                print("读取附件存在非编码待解决问题，暂时pass")
            else:
                print("读取附件存在未知错误，待验证")
            return
        except FileNotFoundError as e:
            print("该文件是错误文件，无法读取处理")
            return
        except TypeError as e:
            print("该文件内容为None，为垃圾文件，pass")
            return
        atts.append({
            "filename": filename,
            "name": attr_name,
            "type": attr_name.split(".")[-1],
            "size": os.path.getsize(attr_fp)
        })
    elif content_type in ('text/plain', 'text/html'):
        is_char = charset
        if is_char is None:
            is_char = "utf-8"
        try:
            text = part.get_payload(decode=True).decode(is_char.lower(), errors="ignore")
        except (UnicodeDecodeError, LookupError):
            try:
                text = part.get_payload(decode=True).decode("utf-8", errors="ignore")
            except UnicodeDecodeError:
                text = part.get_payload(decode=True).decode("iso-8859-1", errors="ignore")

        parts.append({
            "mimeType": content_type,
            "content": text
        })


def process_email(file_path):
    with open(file_path, 'rb') as f:
        raw_msg = f.read()
        msg = email.message_from_bytes(raw_msg)

    # 处理邮件头
    headers = {}
    for key, value in msg.items():
        headers[key] = value

    # 处理邮件正文和附件
    parts = []
    atts = []

    # 判断是否为多部分邮件
    if msg.is_multipart():
        for part in msg.walk():
            parse_multipart(part, parts, atts)
    else:
        parse_single_body(msg, parts, atts, file_path)
