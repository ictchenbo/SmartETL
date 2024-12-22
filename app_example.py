if __name__ == '__main__':
    import sys

    from wikidata_filter.flow_engine import run
    from wikidata_filter.loader import JsonLine
    from wikidata_filter.iterator import *
    from wikidata_filter.iterator.wikidata import *
    from wikidata_filter.iterator.matcher.wikidata import WikidataMatchNew
    from wikidata_filter.util.dicts import from_json

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    wd_base = '/data/users/chenbo/data/wikidata/20240626'
    wiki_base = '/data/users/chenbo/data/wikipedia/20240620'

    id_name_cache = from_json(f"{wd_base}/id-name.json", key_key='id', val_key='name')
    zhwiki_abstract = from_json(f'{wiki_base}/zhwiki-page.json', key_key='title', val_key='abstract')
    enwiki_abstract = from_json(f'{wiki_base}/enwiki-page.json', key_key='title', val_key='abstract')

    kv = {
        'zhwiki': zhwiki_abstract,
        'enwiki': enwiki_abstract
    }

    print('total id-name KV', len(id_name_cache))
    print('total zhwiki title-abstract KV', len(zhwiki_abstract))
    print('total enwiki title-abstract KV', len(enwiki_abstract))

    chains = Chain(
        Filter(WikidataMatchNew({"P31": ["Q18643213", "Q728", "Q7978115", "Q2031121", "Q17205", "Q1186981", "Q216916"]})),
        PropsFilter(props_set_file='config/props_weapon.txt'),
        ObjectNameInject(id_name_cache),
        ItemAbstractInject(kv),
        ChineseSimple(),
        WriteJson(output_file),
        Count()
    )

    run(JsonLine(input_file), chains)
