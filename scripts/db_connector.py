"""
Module de connexion aux bases de données SQL Server et Neo4j.
Lit les credentials depuis le fichier .env à la racine du projet.
"""

import os
import sys
from pathlib import Path

import pyodbc
from dotenv import load_dotenv
from py2neo import Graph

# Charger le fichier .env depuis la racine du projet
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"

if not env_path.exists():
    print(f"❌ Le fichier .env est manquant: {env_path}")
    sys.exit(1)

load_dotenv(env_path, override=True)


class DatabaseConnector:
    """Gestionnaire de connexions aux bases de données."""

    def __init__(self):
        """Initialise les paramètres de connexion depuis les variables d'environnement."""
        # Paramètres SQL Server
        self.sql_server = os.getenv("TPBDD_SERVER")
        self.sql_database = os.getenv("TPBDD_DB")
        self.sql_username = os.getenv("TPBDD_USERNAME")
        self.sql_password = os.getenv("TPBDD_PASSWORD")
        self.odbc_driver = os.getenv("ODBC_DRIVER")

        # Paramètres Neo4j
        self.neo4j_server = os.getenv("TPBDD_NEO4J_SERVER")
        self.neo4j_user = os.getenv("TPBDD_NEO4J_USER")
        self.neo4j_password = os.getenv("TPBDD_NEO4J_PASSWORD")

        self._sql_conn = None
        self._neo4j_graph = None

    def get_sql_connection(self):
        """
        Retourne une connexion à la base SQL Server.

        Returns:
            pyodbc.Connection: Connexion à SQL Server
        """
        if self._sql_conn is None or self._sql_conn.closed:
            connection_string = (
                f"DRIVER={self.odbc_driver};"
                f"SERVER=tcp:{self.sql_server};"
                f"PORT=1433;"
                f"DATABASE={self.sql_database};"
                f"UID={self.sql_username};"
                f"PWD={self.sql_password}"
            )
            self._sql_conn = pyodbc.connect(connection_string)
            print(f"✅ Connecté à SQL Server: {self.sql_database}")

        return self._sql_conn

    def get_neo4j_graph(self):
        """
        Retourne une connexion à la base Neo4j.

        Returns:
            py2neo.Graph: Objet Graph pour Neo4j
        """
        if self._neo4j_graph is None:
            self._neo4j_graph = Graph(
                self.neo4j_server, auth=(self.neo4j_user, self.neo4j_password)
            )
            print(f"✅ Connecté à Neo4j: {self.neo4j_server}")

        return self._neo4j_graph

    def test_connections(self):
        """Teste les connexions aux deux bases de données."""
        print("\n🔍 Test des connexions...\n")

        # Test SQL Server
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print("✅ SQL Server: Connexion réussie")
            version_first_line = version.split('\n')[0]
            print(f"   Version: {version_first_line}")
        except Exception as e:
            print("❌ SQL Server: Erreur de connexion")
            print(f"   {e}")

        # Test Neo4j
        try:
            graph = self.get_neo4j_graph()
            result = graph.run("RETURN 'Neo4j connecté!' AS message").data()
            print("✅ Neo4j: Connexion réussie")
            print(f"   {result[0]['message']}")
        except Exception as e:
            print("❌ Neo4j: Erreur de connexion")
            print(f"   {e}")

    def close(self):
        """Ferme les connexions ouvertes."""
        if self._sql_conn and not self._sql_conn.closed:
            self._sql_conn.close()
            print("🔒 Connexion SQL Server fermée")


def main():
    """Fonction principale - exemple d'utilisation."""
    db = DatabaseConnector()

    # Tester les connexions
    db.test_connections()

    print("\n📊 Exemples de requêtes:\n")

    # Exemple de requête SQL Server
    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
        tables = cursor.fetchall()
        print("📋 Tables SQL Server (5 premières):")
        for table in tables:
            print(f"   - {table[0]}")
    except Exception as e:
        print(f"❌ Erreur SQL: {e}")

    # Exemple de requête Neo4j
    try:
        graph = db.get_neo4j_graph()
        result = graph.run(
            "MATCH (n) RETURN labels(n) AS labels, count(n) AS count LIMIT 5"
        ).data()
        print("\n📋 Labels Neo4j (5 premiers):")
        for row in result:
            print(f"   - {row['labels']}: {row['count']} nœuds")
    except Exception as e:
        print(f"❌ Erreur Neo4j: {e}")

    # Fermer les connexions
    db.close()


if __name__ == "__main__":
    main()
