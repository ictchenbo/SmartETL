import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re


def get_articles_from_sougou(account_name, page=1):
    url = f"https://weixin.sogou.com/weixin?type=2&query={account_name}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    print("fetching", url)

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    ul = soup.find('ul', class_='news-list')

    articles = []
    for item in ul.find_all('li'):
        title = item.find('h3').get_text()
        link = item.find('a')['href']
        summary = item.find('p', class_='txt-info').get_text()
        date = item.find('span', class_='s2').get_text()

        print(title, link, summary, date)

        # 获取真实文章链接
        real_link = urljoin(url, link)
        print(real_link)
        article_response = requests.get(real_link, headers=headers)
        print(article_response.text)
        real_url = re.search(r'url \+= \'(.*?)\';', article_response.text).group(1)

        articles.append({
            'title': title,
            'url': real_url,
            'summary': summary,
            'date': date
        })

    return articles


# 使用示例
account_name = "道达智库"
articles = get_articles_from_sougou(account_name)
for article in articles:
    print(f"标题: {article['title']}")
    print(f"链接: {article['url']}")
    print(f"摘要: {article['summary']}")
    print(f"日期: {article['date']}")
    print("-" * 50)
