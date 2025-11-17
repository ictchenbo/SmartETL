import json
from smartetl.database.neo4j import Neo4j


def export_node(neo4j: Neo4j, filename: str = 'node.json', labels=None, **kwargs):
    out1 = open(filename, "w", encoding='utf8')

    def for_entity(entity):
        props = entity['n']['properties']
        if 'created_time' in props:
            del props['created_time']
        if 'updated_time' in props:
            del props['updated_time']
        try:
            out1.write(json.dumps(entity['n']))
        except:
            return
        out1.write('\n')

    if labels is not None:
        for label in labels:
            for entity in neo4j.scroll(label=label, **kwargs):
                for_entity(entity)
    else:
        for entity in neo4j.scroll(**kwargs):
            for_entity(entity)

    out1.close()


def export_relation(neo4j: Neo4j, filename: str = 'relation.json', **kwargs):
    out2 = open(filename, "w", encoding='utf8')
    for rel in neo4j.scroll(element_type='relation', **kwargs):
        rel = rel['r']
        out2.write(json.dumps(rel))
        out2.write('\n')
    out2.close()


def export_ontology(neo4j: Neo4j, filename: str = 'ontology-node.json'):
    export_node(neo4j, filename, ['EntityType', 'EventType', 'PropertyType', 'RelationshipType'])
