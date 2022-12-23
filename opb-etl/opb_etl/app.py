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
        # app.delete_all()
        # app.load_organizational_structure()
        app.find_nodes(
            label="ÓrgãoSubordinado",
            mapper=lambda record: f"{record['n']['name']} = ${record['n']['orçamentoNãoRealizado'] / 100.0:,.2f}"
        )
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
                WITH line,
                     toInteger(toFloat(line.`ORÇAMENTO INICIAL (R$)`) * 100.0) as orçamentoInicial,
                     toInteger(toFloat(line.`ORÇAMENTO ATUALIZADO (R$)`) * 100.0) as orçamentoAtualizado,
                     toInteger(toFloat(line.`ORÇAMENTO EMPENHADO (R$)`) * 100.0) as orçamentoEmpenhado,
                     toInteger(toFloat(line.`ORÇAMENTO REALIZADO (R$)`) * 100.0) as orçamentoRealizado
                WITH line, orçamentoInicial, orçamentoAtualizado, orçamentoEmpenhado, orçamentoRealizado,
                     (orçamentoAtualizado - orçamentoInicial) as orçamentoAjuste,
                     (orçamentoAtualizado - orçamentoEmpenhado) as orçamentoNãoEmpenhado,
                     (orçamentoAtualizado - orçamentoRealizado) as orçamentoNãoRealizado
                MERGE (up:UnidadeOrçamentária {{name: line.`NOME UNIDADE ORÇAMENTÁRIA`}})
                    ON CREATE SET up.orçamentoInicial = orçamentoInicial,
                                  up.orçamentoAjuste = orçamentoAjuste,
                                  up.orçamentoAtualizado = orçamentoAtualizado,
                                  up.orçamentoEmpenhado = orçamentoEmpenhado,
                                  up.orçamentoNãoEmpenhado = orçamentoNãoEmpenhado,
                                  up.orçamentoRealizado = orçamentoRealizado,
                                  up.orçamentoNãoRealizado = orçamentoNãoRealizado
                    ON MATCH SET up.orçamentoInicial = up.orçamentoInicial + orçamentoInicial,
                                 up.orçamentoAjuste = up.orçamentoAjuste + orçamentoAjuste,
                                 up.orçamentoAtualizado = up.orçamentoAtualizado + orçamentoAtualizado,
                                 up.orçamentoEmpenhado = up.orçamentoEmpenhado + orçamentoEmpenhado,
                                 up.orçamentoNãoEmpenhado = up.orçamentoNãoEmpenhado + orçamentoNãoEmpenhado,
                                 up.orçamentoRealizado = up.orçamentoRealizado + orçamentoRealizado,
                                 up.orçamentoNãoRealizado = up.orçamentoNãoRealizado + orçamentoNãoRealizado
                MERGE (sub:ÓrgãoSubordinado {{name: line.`NOME ÓRGÃO SUBORDINADO`}})
                    ON CREATE SET sub.orçamentoInicial = orçamentoInicial,
                                  sub.orçamentoAjuste = orçamentoAjuste,
                                  sub.orçamentoAtualizado = orçamentoAtualizado,
                                  sub.orçamentoEmpenhado = orçamentoEmpenhado,
                                  sub.orçamentoNãoEmpenhado = orçamentoNãoEmpenhado,
                                  sub.orçamentoRealizado = orçamentoRealizado,
                                  sub.orçamentoNãoRealizado = orçamentoNãoRealizado
                    ON MATCH SET sub.orçamentoInicial = sub.orçamentoInicial + orçamentoInicial,
                                 sub.orçamentoAjuste = sub.orçamentoAjuste + orçamentoAjuste,
                                 sub.orçamentoAtualizado = sub.orçamentoAtualizado + orçamentoAtualizado,
                                 sub.orçamentoEmpenhado = sub.orçamentoEmpenhado + orçamentoEmpenhado,
                                 sub.orçamentoNãoEmpenhado = sub.orçamentoNãoEmpenhado + orçamentoNãoEmpenhado,
                                 sub.orçamentoRealizado = sub.orçamentoRealizado + orçamentoRealizado,
                                 sub.orçamentoNãoRealizado = sub.orçamentoNãoRealizado + orçamentoNãoRealizado
                MERGE (sup:ÓrgãoSuperior {{name: line.`NOME ÓRGÃO SUPERIOR`}})
                    ON CREATE SET sup.orçamentoInicial = orçamentoInicial,
                                  sup.orçamentoAjuste = orçamentoAjuste,
                                  sup.orçamentoAtualizado = orçamentoAtualizado,
                                  sup.orçamentoEmpenhado = orçamentoEmpenhado,
                                  sup.orçamentoNãoEmpenhado = orçamentoNãoEmpenhado,
                                  sup.orçamentoRealizado = orçamentoRealizado,
                                  sup.orçamentoNãoRealizado = orçamentoNãoRealizado
                    ON MATCH SET sup.orçamentoInicial = sup.orçamentoInicial + orçamentoInicial,
                                 sup.orçamentoAjuste = sup.orçamentoAjuste + orçamentoAjuste,
                                 sup.orçamentoAtualizado = sup.orçamentoAtualizado + orçamentoAtualizado,
                                 sup.orçamentoEmpenhado = sup.orçamentoEmpenhado + orçamentoEmpenhado,
                                 sup.orçamentoNãoEmpenhado = sup.orçamentoNãoEmpenhado + orçamentoNãoEmpenhado,
                                 sup.orçamentoRealizado = sup.orçamentoRealizado + orçamentoRealizado,
                                 sup.orçamentoNãoRealizado = sup.orçamentoNãoRealizado + orçamentoNãoRealizado
                MERGE (up)-[:SubordinadoAoÓrgão]->(sub)
                MERGE (sub)-[:SubordinadoAoÓrgão]->(sup)
            }} IN TRANSACTIONS OF 500 ROWS
            """
        )
        print(result)

    def find_nodes(self, label, mapper):
        result = self._execute_transaction(
            f"""
            MATCH (n:{label})
            RETURN *
            """
        )
        items = sorted(map(mapper, result))
        for i in items:
            print(i)

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
