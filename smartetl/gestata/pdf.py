import os

try:
    import fitz
except:
    print("Error! you need to install pymupdf first: pip install pymupdf")
    raise ImportError("pymupdf not installed")


def split(raw_id: str, filepath: str, output_dir: str, pages_per_file: int = 1, min_pages: int = 1):
    """对输入pdf文件进行拆分，输出到指定目录中
    :param raw_id
    :param filepath
    :param output_dir
    :param pages_per_file
    :param min_pages
    """
    assert pages_per_file >= 1
    assert min_pages >= 1

    filename = os.path.basename(filepath)
    if raw_id is None:
        raw_id = filename.lower()

    doc = fitz.open(filepath)
    max_page = len(doc)
    results = []
    if max_page < min_pages or max_page <= pages_per_file:
        suffix = f'1-{max_page}' if max_page > 1 else '1'
        results.append({
            "_id": f'{raw_id}_{suffix}',
            "id": raw_id,
            "filepath": filepath, 
            "total_pages": max_page,
            "part_number": 1, 
            "start_page": 1,
            "end_page": max_page
        })
        doc.close()
        return results

    if filename.endswith('.pdf'):
        filename = filename[:-4]
    
    total = int(max_page / pages_per_file)
    if max_page % pages_per_file > 0:
        total += 1

    for number in range(total):
        start = number * pages_per_file + 1
        end = min(max_page, start + pages_per_file - 1)

        suffix = f'{start}-{end}' if start < end else str(start)
        output_path = os.path.join(output_dir, f'{filename}_{suffix}.pdf')
        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=start-1, to_page=end-1)
        new_doc.save(output_path)
        new_doc.close()
        results.append({
            "_id": f'{raw_id}_{suffix}',
            "id": raw_id,
            "filepath": output_path,
            "total_pages": max_page,
            "part_number": number + 1, 
            "start_page": start,
            "end_page": end
        })
    doc.close()

    return results


if __name__ == '__main__':
    print(split('test', '（英文）皇家联合服务研究所：迎2025年特朗普任总统期间英国和欧洲安全.pdf', './'))
