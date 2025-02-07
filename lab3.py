from neo4j import GraphDatabase, Neo4jDriver
from datetime import datetime

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://0d46ea52.databases.neo4j.io"
AUTH = ("neo4j", "S4U8BeDAcdp3oMgEah4_ngtMm3419CbFOpCG6guUItc")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Successfully connected to Neo4j")


def nodos(lista_nodos):
    with driver.session() as session:
        for nodo in lista_nodos:
            # Obtener el label y las propiedades
            keys = list(nodo.keys())  # Obtener todas las claves
            label = nodo["label"]  # La primera clave es el label
            propiedades = {k: nodo[k] for k in keys[1:]}  # Omitir la primera clave

            # Crear el nodo con sus propiedades
            query_create = f"""
            CREATE (n:{label} $props)
            RETURN n
            """
            session.run(query_create, props=propiedades)
            print(f"Creado nodo {label}")


# Crear driver de forma manual
driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()


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
def crear_relacion(
    node1_label,
    node1_key,
    node1_value,
    node2_label,
    node2_key,
    node2_value,
    relationship_type,
    **properties,
):
    query = f"""
        MATCH (a:{node1_label} {{{node1_key}: $node1_value}}), 
              (b:{node2_label} {{{node2_key}: $node2_value}})
        MERGE (a)-[r:{relationship_type}]->(b)
        SET r += $properties
        RETURN r
        """
    with driver.session() as session:
        r = session.run(
            query,
            node1_value=node1_value,
            node2_value=node2_value,
            properties=properties,
        ).single()
        print(f"Creada relación {relationship_type} entre {node1_value} y {node2_value}")



def buscar_datos(usuario, pelicula):
    with driver.session() as session:
        # Buscar usuario
        query_usuario = "MATCH (u:User {nombre: $nombre}) RETURN u"
        resultado_usuario = session.run(query_usuario, nombre=usuario)
        usuario_encontrado = [record["u"] for record in resultado_usuario]

        # Buscar película
        query_pelicula = "MATCH (m:Movie {titulo: $titulo}) RETURN m"
        resultado_pelicula = session.run(query_pelicula, titulo=pelicula)
        pelicula_encontrada = [record["m"] for record in resultado_pelicula]

        # Buscar relación RATE entre usuario y película
        query_relacion = """
        MATCH (u:User {nombre: $nombre})-[r:RATE]->(m:Movie {titulo: $titulo}) 
        RETURN u, r, m
        """
        resultado_relacion = session.run(
            query_relacion, nombre=usuario, titulo=pelicula
        )
        relacion_encontrada = [record for record in resultado_relacion]

        # Mostrar resultados
        print(f"Usuario encontrado: {usuario_encontrado}")
        print(f"Película encontrada: {pelicula_encontrada}")
        print(f"Relación RATE encontrada: {relacion_encontrada}")


usuarios = [
    {"label": "User", "name": "Juan Perez", "userId": "U1"},
    {"label": "User", "name": "Maria Lopez", "userId": "U2"},
    {"label": "User", "name": "Carlos Gomez", "userId": "U3"},
    {"label": "User", "name": "Ana Martinez", "userId": "U4"},
    {"label": "User", "name": "Luis Rodriguez", "userId": "U5"},
]

peliculas = [
    {"label": "Movie", "title": "Inception", "movieId": 1},
    {"label": "Movie", "title": "The Lion King", "movieId": 2},
    {"label": "Movie", "title": "Gladiator", "movieId": 3},
]


relacion_generos = [
    {"movie_title": "Inception", "name": "Action"},
]
# nodos(usuarios)
# nodos(peliculas)

calificaciones = [
    ("U1", 1, 5.0, 1707171717),
    ("U1", 2, 4.5, 1707171717),
    ("U2", 1, 4.8, 1707171717),
    ("U2", 3, 4.2, 1707171717),
    ("U3", 2, 3.5, 1707171717),
    ("U3", 3, 4.0, 1707171717),
    ("U4", 1, 5.0, 1707171717),
    ("U4", 2, 3.8, 1707171717),
    ("U5", 3, 4.7, 1707171717),
    ("U5", 1, 4.6, 1707171717),
]


lista_nodos = [

 {
        "label": "Director",
        "name": "Christopher Nolan",
        "tmdbld": 12345,
        "born": "1970-07-30",
        "died": "N/A",
        "bomln": "Londres, Reino Unido",
        "url": "https://www.themoviedb.org/person/525",
        "ibmdbld": 12345,
        "bio": 'Christopher Edward Nolan es un director de cine, guionista y productor británico, conocido por sus trabajos en películas de gran éxito como "Inception", "Interstellar" y "The Dark Knight".',
        "poster": "https://link-al-poster.com/nolan.jpg",
    },
    {
        "label": "Actor",
        "name": "Leonardo DiCaprio",
        "tmdbld": 67890,
        "born": "1974-11-11",
        "died": "N/A",
        "bomln": "Los Ángeles, California, USA",
        "url": "https://www.themoviedb.org/person/6193",
        "ibmdbld": 123456,
        "bio": 'Leonardo Wilhelm DiCaprio es un actor, productor y ambientalista estadounidense, conocido por sus roles en películas como "Titanic", "Inception" y "The Revenant".',
        "poster": "https://link-al-poster.com/dicaprio.jpg",
    },
    {
        "label": "User",
        "name": "Juan Perez",
        "userId": 1,
    },
    {
        "label": "PersonDirectorActor",
        "name": "Steven Spielberg",
        "tmdbld": 55555,
        "born": "1946-12-18",
        "died": "N/A",
        "bomln": "Cincinnati, Ohio, USA",
        "url": "https://www.themoviedb.org/person/488",
        "ibmdbld": 12345,
        "bio": 'Steven Spielberg es un director, productor y guionista estadounidense, considerado uno de los directores más exitosos y reconocidos en la historia del cine. Algunas de sus películas más conocidas incluyen "Jaws", "E.T.", "Jurassic Park" y "Schindler\'s List".',
        "poster": "https://link-al-poster.com/spielberg.jpg",
    },
    {
        "label": "Movie",
        "title": "Inception",
        "tmdbld": "499",
        "released": "2010-07-16",
        "imdbRating": 8.8,
        "movieId": 148,
        "year": 2010,
        "imdbld": 1392190,
        "runtime": 148,
        "countries": ["Estados Unidos", "Reino Unido"],
        "imdbVotes": 1645000,
        "url": "https://www.themoviedb.org/movie/499",
        "revenue": 825500000,
        "plot": 'Un "extractor" de información que puede entrar en los sueños de las personas para robar o plantar ideas es contratado para realizar una tarea imposible: plantar una idea en la mente de una persona.',
        "poster": "https://link-al-poster.com/inception.jpg",
        "budget": 160000000,
        "languages": ["Inglés"],
    },
    {
        "label": "Genre",
        "name": "Action"
    }
    

]

nodos(lista_nodos)

crear_relacion("Actor", "name", "Leonardo DiCaprio", "Movie", "movieId", 148, "ACTED_IN", role="Jose Rodriguez")
crear_relacion("Director", "name", "Christopher Nolan", "Movie", "movieId", 148, "DIRECTED", role="Director")
crear_relacion("PersonDirectorActor", "name", "Steven Spielberg", "Movie", "movieId", 148, "DIRECTED", role="Director")
crear_relacion("PersonDirectorActor", "name", "Steven Spielberg", "Movie", "movieId", 148, "ACTED_IN", role="Director")
crear_relacion("User", "userId", 1, "Movie", "movieId", 148, "RATED", rating=4.5, timestamp=14141414)
crear_relacion("Movie", "movieId", 148, "Genre", "name", "Action", "IN_GENRE")

# for user, movie, rating, timestamp in calificaciones:
#     crear_relacion(
#         "User", "userId", user, "Movie", "movieId", movie, "RATED", rating=rating, timestamp=timestamp
#     )

# for relacion in relacion_generos: 
#     crear_relacion(
#         "Movie", "title", relacion["movie_title"],"Genre", "name", relacion["name"],"IN_GENRE"
#     )


driver.close()
