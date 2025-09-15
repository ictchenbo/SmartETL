from io import StringIO
import re


def from_text(text: str):
    """基于CNKI导出的文本格式转化为dict"""
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


def patent_entity(patent: dict):
    """CNKI专利信息转化为KMGS实体结构"""
    title = patent.get('Title')
    return {
        '_id': patent.get('PubNo'),
        'name': title,
        'types': ['Patent'],
        'properties': {
            'title': title,
            'pubno': patent.get('PubNo'),
            'author': patent.get('Author'),
            'applicant': patent.get('Applicant'),
            'country': patent.get('CountryName'),
            'pubtime': patent.get('PubTime'),
            'summary': patent.get('Summary'),
            'claims': patent.get('Claims'),
            'clc': patent.get('CLC')
        }
    }


def __split(s: str) -> list:
    if not s:
        return []
    return re.split('[,;]+', s)


def patent_entity_author(patent: dict):
    """CNKI专利作者实体列表"""
    authors = __split(patent.get('Author'))
    return [{'name': author, 'types': ['Person']} for author in authors]


def patent_entity_org(patent: dict, org_key='Applicant'):
    """CNKI专利机构实体列表"""
    applicants = __split(patent.get(org_key))
    return [{'name': applicant, 'types': ['Organization']} for applicant in applicants]


def patent_relation(patent: dict, org_key='Applicant'):
    """CNKI专利或论文关系"""
    authors = __split(patent.get('Author'))
    applicants = __split(patent.get(org_key))
    title = patent.get('Title')
    relations = []
    for author in authors:
        # 专利-作者
        relations.append({'h': author, 't': title, 'r': 'Publish'})
    for applicant in applicants:
        # 专利-单位
        relations.append({'h': applicant, 't': title, 'r': 'Apply'})

    if len(applicants) == len(authors):
        # 作者-单位-工作关系
        for author, applicant in zip(authors, applicants):
            relations.append({'h': author, 't': applicant, 'r': 'Belong'})
    else:
        if len(applicants) == 1:
            applicant = applicants[0]
            # 作者-单位-工作关系
            for author in authors:
                relations.append({'h': author, 't': applicant, 'r': 'Belong'})
            # 作者-作者-同事关系
            for i in range(len(authors)-1):
                for j in range(len(authors), i+1):
                    relations.append({'h': authors[i], 't': authors[j], 'r': 'Colleague'})
        else:
            # 作者-单位-可能工作
            for author in authors:
                for applicant in applicants:
                    relations.append({'h': author, 't': applicant, 'r': 'Belong_possible'})
            # 作者-单位-可能同事
            for i in range(len(authors)-1):
                for j in range(len(authors), i+1):
                    relations.append({'h': authors[i], 't': authors[j], 'r': 'Colleague_possible'})

    if len(applicants) > 1:
        for i in range(len(applicants)-1):
            for j in range(len(applicants), i+1):
                relations.append({'h': applicants[i], 't': applicants[j], 'r': 'Cooperate'})

    return relations


def paper_entity(paper: dict):
    """CNKI论文转化为KMGS实体结构"""
    title = paper.get('Title')
    s_kw = paper.get('Keyword')
    return {
        'name': title,
        'types': ['Paper'],
        'properties': {
            'title': title,
            'author': paper.get('Author'),
            'applicant': paper.get('Organ'),
            'source': paper.get('Source'),
            'keywords': s_kw.split(';') if s_kw else [],
            'summary': paper.get('Summary'),
            'pubtime': paper.get('PubTime'),
            'year': paper.get('Year'),
            'volume': paper.get('Volume'),
            'period': paper.get('Period'),
            'clc': paper.get('CLC'),
            'url': paper.get('URL'),
            'doi': paper.get('DOI')
        }
    }
