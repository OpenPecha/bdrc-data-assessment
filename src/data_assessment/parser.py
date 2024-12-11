import rdflib
from neo4j import GraphDatabase
import os

uri = os.getenv('neo4j_uri')
username = os.getenv('neo4j_username')
password = os.getenv('neo4j_password')

driver = GraphDatabase.driver(uri, auth=(username, password))


def load_trig_file(file_path):
    g = rdflib.ConjunctiveGraph()
    g.parse(file_path, format="trig")
    return g


def upload_to_neo4j(tx, subj, pred, obj):
    """
    Upload the RDF triple to Neo4j as nodes and relationships.
    """
    tx.run("MERGE (s:Resource {uri: $subj})", subj=subj)
    tx.run("MERGE (o:Resource {uri: $obj})", obj=obj)
    tx.run("""
    MATCH (s:Resource {uri: $subj}), (o:Resource {uri: $obj})
    MERGE (s)-[r:RELATES_TO {predicate: $pred}]->(o)
    """, subj=subj, obj=obj, pred=pred)


def check_subject_exists(tx, subj):
    result = tx.run("MATCH (s:Resource {uri: $subj}) RETURN s", subj=subj)
    return result.single() is not None


def upload_rdf_graph_to_neo4j(rdf_graph, target_subject):
    """
    Upload RDF data to Neo4j, filtering by target subject, skipping already uploaded data.
    """
    with driver.session() as session:
        for subj, pred, obj in rdf_graph:
            subj_label = str(subj)
            if str(subj) == target_subject:
                if not session.execute_read(check_subject_exists, subj_label):
                    pred_label = str(pred)
                    obj_label = str(obj)
                    session.execute_write(upload_to_neo4j, subj_label, pred_label, obj_label)
                    print(f"Uploaded RDF data for {subj_label} to Neo4j.")
                else:
                    print(f"Subject {subj_label} already exists in the database, skipping.")
            else:
                print(f"Skipping RDF triple for {subj_label}, not the target subject.")


def process_trig_files_in_directory(root_dir):
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".trig"):
                trig_file_path = os.path.join(subdir, file)
                file_name = os.path.basename(trig_file_path).split(".")[0]
                target_subject = f"http://purl.bdrc.io/resource/{file_name}"
                rdf_graph = load_trig_file(trig_file_path)
                upload_rdf_graph_to_neo4j(rdf_graph, target_subject)

                print(f"Processed {file}.")


if __name__ == "__main__":
    root_directory = "data/instances-20220922"
    process_trig_files_in_directory(root_directory)
