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
