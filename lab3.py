from neo4j import GraphDatabase, Neo4jDriver

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://801ec156.databases.neo4j.io"
AUTH = ("neo4j", "VY88ReoU_3qwDWfzyfZd0YCFETiN-8C_-4zTfPSiMoI")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Successfully connected to Neo4j")

def nodos(lista_nodos):
    with driver.session() as session:
        for nodo in lista_nodos:
            # Obtener el label y las propiedades
            keys = list(nodo.keys())  # Obtener todas las claves
            label = keys[0]  # La primera clave es el label
            propiedades = {k: nodo[k] for k in keys[1:]}  # Omitir la primera clave
            
            # Contar nodos con el mismo label
            query_count = f"MATCH (p:{label}) RETURN count(p) AS total"
            result = session.run(query_count)
            nodo_id = 0
            for record in result:
                nodo_id = record['total']
            
            # Crear el nodo con sus propiedades
            query_create = f"""
            MERGE (n:{label} $props)
            RETURN n
            """
            session.run(query_create, props=propiedades)
            print(f"Creado nodo {label} con ID {nodo_id}")
# Crear driver de forma manual
driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()
print("Successfully connected to Neo4j")


"""
    Crea una relación entre dos nodos.

    :param node1_label: Etiqueta del nodo de origen (ej. "User").
    :param node1_key: Clave única del nodo de origen (ej. "userId").
    :param node1_value: Valor del nodo de origen (ej. "U123").
    :param node2_label: Etiqueta del nodo de destino (ej. "Movie").
    :param node2_key: Clave única del nodo de destino (ej. "movieId").
    :param node2_value: Valor del nodo de destino (ej. 1).
    :param relationship_type: Tipo de relación (ej. "RATED").
    :param properties: Propiedades adicionales de la relación (ej. rating=5, timestamp=1707171717).
"""
def crear_relacion(node1_label, node1_key, node1_value, node2_label, node2_key, node2_value, relationship_type, **properties):
        query = f"""
        MATCH (a:{node1_label} {{{node1_key}: $node1_value}}), 
              (b:{node2_label} {{{node2_key}: $node2_value}})
        MERGE (a)-[r:{relationship_type}]->(b)
        SET r += $properties
        RETURN r
        """
        with driver.session() as session:
            return session.run(query, node1_value=node1_value, node2_value=node2_value, properties=properties).single()
        
crear_relacion("User", "userId", "d2", "Movie", "MovieId", 1, "Rated", rating=5, timestamp=1707171718)

driver.close()
