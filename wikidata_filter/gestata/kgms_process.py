from wikidata_filter.util.files import json_lines, write_json_lines


def compress_triples(json_file: str, output_file: str):
    records = {}
    for row in json_lines(json_file):
        e = row["e"]
        a = row["a"]
        v = row["v"]
        if e not in records:
            records[e] = {"e": e, a: v}
        else:
            records[e][a] = v
    write_json_lines(output_file, records.values())


def json_join(main_json_file: str, main_key: str, join_json_file: str, join_key: str = None, output_file: str = None):
    records = {}
    for row in json_lines(main_json_file):
        if main_key not in row:
            continue
        pk = row[main_key]
        records[pk] = row

    join_key = join_key or main_key
    for row in json_lines(join_json_file):
        if join_key not in row:
            continue
        fk = row.pop(join_key)
        if fk not in records:
            print("record not found:", fk)
            continue
        records[fk].update(**row)

    write_json_lines(output_file or main_json_file, records.values())


if __name__ == '__main__':
    path = '../../data/kg/wiki/en_triplets'
    compress_triples(f'{path}/entity_attributes.json', f'{path}/entity_props.json')
    json_join(f'{path}/entity_labels.json', 'e', f'{path}/entity_props.json', 'e', f'{path}/entity_merged.json')
    json_join(f'{path}/entity_merged.json', 'e', f'{path}/entity_categories.json')
