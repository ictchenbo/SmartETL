AUTHOR_PATTERN = [
            "责编[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "责任编辑[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "作者[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "编辑[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "文[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "原创[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "撰文[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：]",
            "来源[：|:| |丨|/]\s*([\u4E00-\u9FA5a-zA-Z]{2,20})[^\u4E00-\u9FA5|:|：|<]",
            # 以下正则表达式需要进一步测试
            # '(作者[：|:| |丨|/]\s*[\u4E00-\u9FA5a-zA-Z、 ]{2,20})[）】)]]?[^\u4E00-\u9FA5|:|：]',
            # '(记者[：|:| |丨|/]\s*[\u4E00-\u9FA5a-zA-Z、 ]{2,20})[）】)]]?[^\u4E00-\u9FA5|:|：]',
            # '(原创[：|:| |丨|/]\s*[\u4E00-\u9FA5a-zA-Z、 ]{2,20})[）】)]]?[^\u4E00-\u9FA5|:|：]',
            # '(撰文[：|:| |丨|/]\s*[\u4E00-\u9FA5a-zA-Z、 ]{2,20})[）】)]]?[^\u4E00-\u9FA5|:|：]',
            # '(文/图[：|:| |丨|/]?\s*[\u4E00-\u9FA5a-zA-Z、 ]{2,20})[）】)]]?[^\u4E00-\u9FA5|:|：]',
]

DATETIME_PATTERN = [
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4})",  # 英文月日年
    r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",  # 英文日月年
    r"(\d{4}-\d{2}-\d{2}(T|\s+)\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})\s+\w?)",  # UTC时间戳格式
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9])",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[2][0-3]:[0-5]?[0-9])",
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2})",
    "(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    "(\d{2}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    "(\d{4}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{4}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{4}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    "(\d{4}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    "(\d{4}年\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    "(\d{2}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{2}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{2}年\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    "(\d{2}年\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    "(\d{2}年\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    "(\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9]:[0-5]?[0-9])",
    "(\d{1,2}月\d{1,2}日\s*?[0-1]?[0-9]:[0-5]?[0-9])",
    "(\d{1,2}月\d{1,2}日\s*?[2][0-3]:[0-5]?[0-9])",
    "(\d{1,2}月\d{1,2}日\s*?[1-24]\d时[0-60]\d分)([1-24]\d时)",
    "(\d{4}年\d{1,2}月\d{1,2}日)",
    "(\d{2}年\d{1,2}月\d{1,2}日)"
]

UTC_FORMAT = r"(\d{4}-\d{2}-\d{2}(T|\s+)\d{2}(:\d{2}){1,2}(Z|[+-]\d{2}:\d{2})(\s+\w)?)"

DATETIME_PATTERN_V2 = [
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\s+\d{2}(:\d{2}){1,2}(\s+[AP]M)?)",  # 英文月日年
    r"(\d{1,2}(st|nd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}\s+\d{2}:\d{2})",  # 英文日月年
    r"((?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4})",  # 英文月日年
    r"(\d{1,2}(st|nd|th)?\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})",  # 英文日月年
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?\d{2}:\d{2}:\d{2}(\s+[AP]M)?)",
    r"(\d{4}[-|/|.]\d{1,2}[-|/|.]\d{1,2}\s*?\d{2}:\d{2}(\s+[AP]M)?)",
]

DATETIME_PATTERN_URL = r'(?<=\W)([\./\-_]{0,1}(19|20)\d{2})[\./\-_]{0,1}(([0-3]{0,1}[0-9][\./\-_])|(\w{3,5}[\./\-_]))([0-3]{0,1}[0-9][\./\-]{0,1})?'


TITLE_HTAG_XPATH = '//h1//text() | //h2//text() | //h3//text() | //h4//text()'

TITLE_SPLIT_CHAR_PATTERN = '[-_|]'

USELESS_TAG = ['style', 'script', 'link', 'video', 'iframe', 'source', 'picture', 'header', 'blockquote',
               'footer']

# if one tag in the follow list does not contain any child node nor content, it could be removed
TAGS_CAN_BE_REMOVE_IF_EMPTY = ['section', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span']

USELESS_ATTR = {
                'share',
                'contribution',
                'copyright',
                'copy-right',
                'disclaimer',
                'recommend',
                'related',
                'footer',
                'comment',
                'social',
                'submeta',
                'report-infor',
                'header_toolbar'
                }


HIGH_WEIGHT_ARRT_KEYWORD = ['content',
                            'article',
                            'news_txt',
                            'pages_content',
                            'post_text']


PUBLISH_TIME_META = [  # 部分特别规范的新闻网站，可以直接从 HTML 的 meta 数据中获得发布时间
    '//meta[starts-with(@property, "rnews:datePublished")]/@content',
    '//meta[starts-with(@property, "article:published_time")]/@content',
    '//meta[starts-with(@property, "og:published_time")]/@content',
    '//meta[starts-with(@property, "og:release_date")]/@content',
    '//meta[starts-with(@itemprop, "datePublished")]/@content',
    '//meta[starts-with(@itemprop, "dateUpdate")]/@content',
    '//meta[starts-with(@name, "OriginalPublicationDate")]/@content',
    '//meta[starts-with(@name, "article_date_original")]/@content',
    '//meta[starts-with(@name, "og:time")]/@content',
    '//meta[starts-with(@name, "apub:time")]/@content',
    '//meta[starts-with(@name, "publication_date")]/@content',
    '//meta[starts-with(@name, "sailthru.date")]/@content',
    '//meta[starts-with(@name, "PublishDate")]/@content',
    '//meta[starts-with(@name, "publishdate")]/@content',
    '//meta[starts-with(@name, "PubDate")]/@content',
    '//meta[starts-with(@name, "pubtime")]/@content',
    '//meta[starts-with(@name, "_pubtime")]/@content',
    '//meta[starts-with(@name, "weibo: article:create_at")]/@content',
    '//meta[starts-with(@pubdate, "pubdate")]/@content',
]



PUBLISH_TIME_META_ENDS = [
    "datePublished",
    "published_time",
    "OriginalPublicationDate",
    "PublishDate",
    "publishdate",
    "article_date_original",
    "publication_date",
    "PubDate",
    "pubtime",
    "pubdate",
    "release_date",
    "datecreated",
    "create_at",
    "dateUpdate",
    "date"
]

# updated_time


PUBLISH_TIME_META_2 = {  # 修改为属性名后缀 部分特别规范的新闻网站，可以直接从 HTML 的 meta 数据中获得发布时间
    "property": PUBLISH_TIME_META_ENDS,
    "itemprop": PUBLISH_TIME_META_ENDS,
    "name": PUBLISH_TIME_META_ENDS
}

# 满足下面的XPath，极有可能是文章详情页
ARTICLE_XPATH = [
    '//*[@class="article__content"]',
]

PUBLISH_TIME_LD = 'datePublished'


PUBLISH_TIME_TIME = [
    '//time[@itemprop="datePublished"]/@datetime',
    '//time[@itemprop="datePublished"]/text()',
    '//time[@class="time"]/@datetime',
    '//time[@class="time"]/text()',
    '//time[@class="pubdate"]/@datetime',
    '//time[@class="pubdate"]/text()'
]

PUBLISH_TIME_OTHER = [
    # '//span[@class="time" or @class="date"]/text()',
    # '//div[@class="time" or @class="date"]/text()',
    '//span[@id="publish_time"]/text()',
    '//div[@id="publish_time"]/text()',
    '//div[@data-pubtime]/@data-pubtime',
    '//span[@data-time]/@data-time'
]
