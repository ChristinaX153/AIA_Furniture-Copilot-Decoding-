from neo4j import GraphDatabase
import pandas as pd

#Neo4j
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "123456789"  # Replace with your actual password

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

#GDS ---
def project_graph():
    query = """
    CALL gds.graph.project(
        'furnitureGraph',
        ['Furniture', 'BuildingElement'],
        {
            CONNECTED_TO: {type: 'CONNECTED_TO'},
            CLOSEST_TO: {type: 'CLOSEST_TO'}
        }
    )
    """
    with driver.session() as session:
        session.run("CALL gds.graph.drop('furnitureGraph', false)")  # drop old one if exists
        session.run(query)
    print("Graph projected.")

#Assign Components
def assign_components():
    query = """
    CALL gds.wcc.write('furnitureGraph', {
        writeProperty: 'componentId'
    })
    """
    with driver.session() as session:
        session.run(query)
    print("Components assigned.")

#Set Abchors
def set_anchors():
    query = """
    MATCH (f:Furniture)-[:CLOSEST_TO]->(b)
    SET f.anchor = b.id,
        f.anchor_type = head(labels(b))
    """
    with driver.session() as session:
        session.run(query)
    print("Anchors set.")

#Assgn Ranks
def assign_ranks():
    query = """
    MATCH (f:Furniture)
    OPTIONAL MATCH (f)-[:CONNECTED_TO]->(n)
    WITH f, COUNT(n) AS connections
    SET f.rank = CASE 
        WHEN connections = 0 THEN 1
        WHEN connections = 1 THEN 2
        ELSE 3
    END
    """
    with driver.session() as session:
        session.run(query)
    print("Ranks assigned.")

#CSV and JSON
def export_component(component_id):
    query = """
    MATCH (f:Furniture)
    RETURN f.id, f.label, f.anchor, f.anchor_type, f.rank
    ORDER BY f.rank
    """
    with driver.session() as session:
        result = session.run(query)
        df = pd.DataFrame([r.data() for r in result])
        df.to_csv(f"component_{component_id}_decoded.csv", index=False)
        df.to_json(f"component_{component_id}_decoded.json", orient="records", indent=2)
    print(f"Exported component {component_id} to CSV and JSON.")

#run
print("Projecting graph...")
project_graph()

print("Assigning components...")
assign_components()

print("Tagging anchors...")
set_anchors()

print("Assigning ranks...")
assign_ranks()

print("Exporting component 0...")
export_component(0)
