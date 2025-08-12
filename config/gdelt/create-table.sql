CREATE TABLE gdelt_events (
    -- EVENTID AND DATE ATTRIBUTES (事件ID和日期属性)
    GlobalEventID Int64 COMMENT '事件的全球唯一标识符',
    `Day` Int32 COMMENT '事件发生日期 (YYYYMMDD格式)',
    MonthYear Int32 COMMENT '事件发生月份 (YYYYMM格式)',
    `Year` Int16 COMMENT '事件发生年份 (YYYY格式)',
    FractionDate Float32 COMMENT '事件发生日期的分数表示 (YYYY.FFFF)',

    -- ACTOR ATTRIBUTES (Actor1) (参与者属性 - 参与者1)
    Actor1Code String COMMENT '参与者1的完整CAMEO代码',
    Actor1Name String COMMENT '参与者1的实际名称',
    Actor1CountryCode String COMMENT '参与者1的国家代码',
    Actor1KnownGroupCode String COMMENT '参与者1的已知组织代码',
    Actor1EthnicCode String COMMENT '参与者1的民族代码',
    Actor1Religion1Code String COMMENT '参与者1的主要宗教代码',
    Actor1Religion2Code String COMMENT '参与者1的次要宗教代码',
    Actor1Type1Code String COMMENT '参与者1的类型/角色代码1',
    Actor1Type2Code String COMMENT '参与者1的类型/角色代码2',
    Actor1Type3Code String COMMENT '参与者1的类型/角色代码3',

    -- ACTOR ATTRIBUTES (Actor2) (参与者属性 - 参与者2)
    Actor2Code String COMMENT '参与者2的完整CAMEO代码',
    Actor2Name String COMMENT '参与者2的实际名称',
    Actor2CountryCode String COMMENT '参与者2的国家代码',
    Actor2KnownGroupCode String COMMENT '参与者2的已知组织代码',
    Actor2EthnicCode String COMMENT '参与者2的民族代码',
    Actor2Religion1Code String COMMENT '参与者2的主要宗教代码',
    Actor2Religion2Code String COMMENT '参与者2的次要宗教代码',
    Actor2Type1Code String COMMENT '参与者2的类型/角色代码1',
    Actor2Type2Code String COMMENT '参与者2的类型/角色代码2',
    Actor2Type3Code String COMMENT '参与者2的类型/角色代码3',

    -- EVENT ACTION ATTRIBUTES (事件动作属性)
    IsRootEvent Int8 COMMENT '是否为文档中的根事件 (1=是, 0=否)',
    EventCode String COMMENT '事件的原始CAMEO动作代码',
    EventBaseCode String COMMENT '事件的CAMEO基础代码 (二级分类)',
    EventRootCode String COMMENT '事件的CAMEO根代码 (一级分类)',
    QuadClass Int8 COMMENT '事件的四分类: 1=言语合作, 2=物质合作, 3=言语冲突, 4=物质冲突',
    GoldsteinScale Float32 COMMENT '事件的Goldstein规模分 (范围-10到+10)',
    NumMentions Int32 COMMENT '该事件在首次出现的15分钟内的总提及次数 (遗留字段)',
    NumSources Int32 COMMENT '包含该事件提及的独立信息源数量 (遗留字段)',
    NumArticles Int32 COMMENT '包含该事件提及的源文档总数 (遗留字段)',
    AvgTone Float32 COMMENT '包含该事件提及的文档的平均情感值 (-100到+100)',

    -- EVENT GEOGRAPHY (Actor1) (事件地理信息 - 参与者1)
    Actor1Geo_Type Int8 COMMENT '参与者1地理匹配类型: 1=国家, 2=美国州, 3=美国城市, 4=世界城市, 5=世界州/省',
    Actor1Geo_Fullname String COMMENT '参与者1地理匹配位置的全称',
    Actor1Geo_CountryCode String COMMENT '参与者1地理匹配位置的国家代码 (FIPS10-4)',
    Actor1Geo_ADM1Code String COMMENT '参与者1地理匹配位置的行政一级代码 (如州/省)',
    Actor1Geo_ADM2Code String COMMENT '参与者1地理匹配位置的行政二级代码 (如县/市)',
    Actor1Geo_Lat Float64 COMMENT '参与者1地理匹配位置的纬度',
    Actor1Geo_Long Float64 COMMENT '参与者1地理匹配位置的经度',
    Actor1Geo_FeatureID String COMMENT '参与者1地理匹配位置的GNS/GNIS特征ID',

    -- EVENT GEOGRAPHY (Actor2) (事件地理信息 - 参与者2)
    Actor2Geo_Type Int8 COMMENT '参与者2地理匹配类型: 1=国家, 2=美国州, 3=美国城市, 4=世界城市, 5=世界州/省',
    Actor2Geo_Fullname String COMMENT '参与者2地理匹配位置的全称',
    Actor2Geo_CountryCode String COMMENT '参与者2地理匹配位置的国家代码 (FIPS10-4)',
    Actor2Geo_ADM1Code String COMMENT '参与者2地理匹配位置的行政一级代码 (如州/省)',
    Actor2Geo_ADM2Code String COMMENT '参与者2地理匹配位置的行政二级代码 (如县/市)',
    Actor2Geo_Lat Float64 COMMENT '参与者2地理匹配位置的纬度',
    Actor2Geo_Long Float64 COMMENT '参与者2地理匹配位置的经度',
    Actor2Geo_FeatureID String COMMENT '参与者2地理匹配位置的GNS/GNIS特征ID',

    -- EVENT GEOGRAPHY (Action) (事件地理信息 - 动作)
    ActionGeo_Type Int8 COMMENT '动作地理匹配类型: 1=国家, 2=美国州, 3=美国城市, 4=世界城市, 5=世界州/省',
    ActionGeo_Fullname String COMMENT '动作发生位置的全称',
    ActionGeo_CountryCode String COMMENT '动作发生位置的国家代码 (FIPS10-4)',
    ActionGeo_ADM1Code String COMMENT '动作发生位置的行政一级代码 (如州/省)',
    ActionGeo_ADM2Code String COMMENT '动作发生位置的行政二级代码 (如县/市)',
    ActionGeo_Lat Float64 COMMENT '动作发生位置的纬度',
    ActionGeo_Long Float64 COMMENT '动作发生位置的经度',
    ActionGeo_FeatureID String COMMENT '动作发生位置的GNS/GNIS特征ID',

    -- DATA MANAGEMENT FIELDS (数据管理字段)
    DATEADDED DateTime COMMENT '事件被添加到主数据库的日期和时间 (YYYYMMDDHHMMSS格式)',
    SOURCEURL String COMMENT '首次发现该事件的新闻报告的URL或引用'
)
ENGINE = MergeTree()
ORDER BY (`Year`, MonthYear, `Day`, GlobalEventID) -- 按年、月、日和事件ID排序，优化时间范围查询
COMMENT '存储GDELT全球事件数据库的主表';
