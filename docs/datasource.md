## 数据资源
1. wikipedia
2. wikidata
3. GDELT（全球事件库）
4. GTD（全球恐怖主义事件库）
5. 境外新闻（基于GDELT源采集）
6. 开源情报报告（通过情报分析师的知识星球下载）
7. 境外新闻图片（基于境外新闻URL爬取）
8. 民调数据（经济学人美国大选专题）
9. 预测市场数据（手工收集polymarket、smarkets等预测市场平台）
10. OpenSanctions数据库 包括全球制裁实体名单或涉政治、犯罪与经济重点人物/公司等
11. 联合国教科文组织项目数据
12. FourSqure全球POI数据


### 文件下载类
1. wikidata(json dump文件)
2. wikipedia(xml dump文件)
3. GDELT(每15分钟一个csv.zip数据包)  流程示例：[GDELT下载](../flows/gdelt.yaml)
4. GTD（excel文件） 流程示例：[GTD处理](../flows/gtd.yaml)
5. Nuclei 模板文件(yaml文件)（https://github.com/projectdiscovery/nuclei-templates） 流程示例：[漏洞POC描述及POC生成](../flows/nl2poc/nuclei_http_poc_desc.yaml)
6. OpenSanctions数据库(ftm.json格式) 流程示例：[FTM数据处理](../flows/opensanctions_peps.yaml)
7. 联合国教科文组织项目数据（excel文件） 流程示例：[项目统计](../flows/unesco-projects-aggs.yaml)
8. FourSqure全球POI数据(parquet文件) 流程示例：[POI数据处理](../flows/files/file_parquet.yaml)

### API类
1. ReaderAPI 根据网页URL获取网页markdown内容 流程示例：[ReaderAPI](../flows/api_readerapi.yaml)
2. Kafka Web 流程示例：[Kakfa新闻处理](../flows/news/p1_kafka.yaml)
3. 新闻图片 根据新闻的图片URL爬取图片 流程示例：[图片采集](../flows/news/p2_image.yaml)
4. 民调数据 流程示例：[民调数据处理](../flows/polls.yaml)

### 手工收集类
1. 预测市场数据 通过手工收集数据写入文件 流程示例：[预测市场数据处理](../flows/futures.yaml)
2. 开源情报报告（word文件，手工下载） 流程示例：[word新闻解析](../flows/news/load_news_report_doc.yaml)
