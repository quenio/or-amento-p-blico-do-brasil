import logging

from os import getenv

from dotenv import load_dotenv

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

NEO4J_URI = "NEO4J_URI"
NEO4J_USERNAME = "NEO4J_USERNAME"
NEO4J_PASSWORD = "NEO4J_PASSWORD"
DATA_SOURCE_URI = "DATA_SOURCE_URI"


def main():
    load_dotenv()
    neo4j_uri = getenv(NEO4J_URI)
    neo4j_username = getenv(NEO4J_USERNAME)
    neo4j_password = getenv(NEO4J_PASSWORD)
    data_source_uri = getenv(DATA_SOURCE_URI)
    app = App(neo4j_uri, neo4j_username, neo4j_password, data_source_uri)
    try:
        app.find_nodes(label="ÓrgãoSuperior")
    finally:
        app.close()


class App:

    def __init__(self, uri, user, password, data_source_uri):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.data_source_uri = data_source_uri

    def close(self):
        self.driver.close()

    def delete_all(self):
        result = self._execute_transaction(
            """
            MATCH (n1)-[r]-(n2)
            DELETE r, n1, n2
            """
        )
        print(result)

    def load_organizational_structure(self):
        result = self._execute_transaction(
            f"""
            LOAD CSV WITH HEADERS
            FROM '{self.data_source_uri}' AS line
            CALL {{
                WITH line
                MERGE (up:UnidadeOrçamentária {{name: line.`NOME UNIDADE ORÇAMENTÁRIA`}})
                MERGE (sub:ÓrgãoSubordinado {{name: line.`NOME ÓRGÃO SUBORDINADO`}})
                MERGE (sup:ÓrgãoSuperior {{name: line.`NOME ÓRGÃO SUPERIOR`}})
                MERGE (up)-[:SubordinadoAoÓrgão]->(sub)
                MERGE (sub)-[:SubordinadoAoÓrgão]->(sup)
            }} IN TRANSACTIONS OF 500 ROWS
            """
        )
        print(result)

    def find_nodes(self, label):
        result = self._execute_transaction(
            f"""
            MATCH (n:{label})
            RETURN *
            """
        )
        for record in result:
            print(record["n"]["name"])

    def _execute_transaction(self, command):
        with self._start_transaction() as tx:
            try:
                result = tx.run(command)
                return [row for row in result]
            except ServiceUnavailable as exception:
                error_message = "{command} raised an error:\n {exception}"
                logging.error(error_message.format(command=command, exception=exception))
                raise

    def _start_transaction(self):
        return self.driver.session(database="neo4j")
