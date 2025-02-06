from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://801ec156.databases.neo4j.io"
AUTH = ("neo4j", "VY88ReoU_3qwDWfzyfZd0YCFETiN-8C_-4zTfPSiMoI")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Successfully connected to Neo4j")