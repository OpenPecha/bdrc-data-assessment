import rdflib
from neo4j import GraphDatabase
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

uri = os.getenv('neo4j_uri')
username = os.getenv('neo4j_username')
password = os.getenv('neo4j_password')
driver = GraphDatabase.driver(uri, auth=(username, password))


def load_trig_file(file_path):
    g = rdflib.ConjunctiveGraph()
    g.parse(file_path, format="trig")
    return g


def batch_upload_to_neo4j(tx, graph_name, triples):
    """
    Batch upload triples to Neo4j using Cypher UNWIND for performance
    """
    tx.run("""
    UNWIND $triples AS triple
    MERGE (g:Graph {uri: $graph_name})
    MERGE (s:Resource {uri: triple.subj})
    MERGE (o:Resource {uri: triple.obj})
    MERGE (s)-[r:RELATES_TO {predicate: triple.pred}]->(o)
    MERGE (s)-[:PART_OF]->(g)
    MERGE (o)-[:PART_OF]->(g)
    """, graph_name=graph_name, triples=triples)


def upload_named_graph_to_neo4j(rdf_graph):
    with driver.session() as session:
        for graph in rdf_graph.contexts():
            graph_name = str(graph.identifier)
            target_subject = graph_name.replace("http://purl.bdrc.io/graph/", "http://purl.bdrc.io/resource/")

            # Collect triples for batch processing
            triples = []
            for subj, pred, obj in graph:
                subj_label = str(subj)
                if subj_label == target_subject:
                    triples.append({
                        'subj': str(subj),
                        'pred': str(pred),
                        'obj': str(obj)
                    })

            if triples:
                session.execute_write(batch_upload_to_neo4j, graph_name, triples)


def process_single_trig_file(trig_file_path, log_file_path):
    try:
        print(f"Processing file: {trig_file_path}")
        rdf_graph = load_trig_file(trig_file_path)
        upload_named_graph_to_neo4j(rdf_graph)

        # Log processed file
        with open(log_file_path, "a") as log:
            log.write(f"{os.path.basename(trig_file_path)}\n")
        return True
    except Exception as e:
        print(f"Error processing {trig_file_path}: {e}")
        return False


def process_trig_files_in_directory(root_dir, log_file="processed_files.log", max_workers=None):
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    processed_files = set()
    if os.path.exists(log_file):
        with open(log_file, "r") as log:
            processed_files = set(log.read().splitlines())

    files_to_process = []
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".trig") and file not in processed_files:
                files_to_process.append(os.path.join(subdir, file))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        full_log_file_path = os.path.abspath(log_file)

        results = list(executor.map(
            process_single_trig_file,
            files_to_process,
            [full_log_file_path] * len(files_to_process)
        ))

    successful = sum(results)
    print(f"Processed {successful} out of {len(files_to_process)} files.")


if __name__ == "__main__":
    root_directory = "data/instances-20220922"
    process_trig_files_in_directory(root_directory)
