from smartetl.gestata.kgms_dba import *


if __name__ == '__main__':
    neo4j = Neo4j(host='bolt://10.170.130.21:7687', password='suyan_root')

    export_ontology(neo4j)
    export_node(neo4j, labels=['Entity'])
    export_relation(neo4j)
