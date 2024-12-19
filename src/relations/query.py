import os
import csv
from neo4j import GraphDatabase
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


uri = os.getenv('neo4j_uri')
username = os.getenv('neo4j_username')
password = os.getenv('neo4j_password')


def execute_query(driver, query, parameters=None):
    with driver.session() as session:
        result = list(session.run(query, parameters or {}))
        return result


def save_results_to_csv(records, output_file):
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["relation", "unique_values", "resource_count", "resource_id"])
        for record in records:
            writer.writerow([
                record["relation"],
                record["unique_values"],
                record["resource_count"],
                ",".join(record["resource_id"])
            ])


def run_query_and_save_to_csv(query, output_file, parameters=None):
    """Connect to Neo4j using environment variables, execute query, and save results."""
    driver = GraphDatabase.driver(uri, auth=(username, password))

    try:
        records = execute_query(driver, query, parameters)
        save_results_to_csv(records, output_file)
        logger.info(f"Query results saved to {output_file}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        driver.close()


if __name__ == "__main__":
    filter_resources = [
        "http://purl.bdrc.io/resource/LangBo",
        "http://purl.bdrc.io/resource/ScriptKhyugYig",
        "http://purl.bdrc.io/resource/ScriptTibt",
        "http://purl.bdrc.io/resource/ScriptDbuCan",
        "http://purl.bdrc.io/resource/ScriptDbuMed"
    ]

    query_template = """
    MATCH (r:Resource)-[:PART_OF]->(g:Graph)
    WHERE r.uri IN $filter_resources
    WITH DISTINCT g

    MATCH (r1:Resource)-[:PART_OF]->(g)
    WHERE r1.uri ENDS WITH SUBSTRING(g.uri, size("http://purl.bdrc.io/graph/"))
    WITH DISTINCT r1, g

    MATCH (r1)-[rel:RELATES_TO {predicate: $predicate}]->(r2:Resource)
    WITH rel.predicate AS relation,
         r2.uri AS unique_values, 
         COUNT(DISTINCT g) AS resource_count, 
         COLLECT(DISTINCT SUBSTRING(g.uri, size("http://purl.bdrc.io/graph/"))) AS resource_id
    RETURN relation, unique_values, resource_count, resource_id
    """

    predicates_file = "data/relatioship.txt"
    output_dir = "data/query_data/"

    if not os.path.exists(predicates_file):
        logger.error(f"The predicates file {predicates_file} does not exist.")
        exit(1)

    with open(predicates_file, 'r') as file:
        predicates = [line.strip() for line in file if line.strip()]

    for predicate in predicates:
        predicate_name = predicate.split("/")[-1]
        output_csv = os.path.join(output_dir, f"{predicate_name}.csv")

        parameters = {
            "filter_resources": filter_resources,
            "predicate": predicate
        }

        logger.info(f"Processing predicate: {predicate}")
        run_query_and_save_to_csv(query_template, output_csv, parameters)
