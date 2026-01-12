"""
Exercice 2: Compter le nombre d'artistes présents dans la base de données
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def count_artists():
    """
    Compte le nombre total d'artistes dans la base de données.

    Returns:
        int: Nombre d'artistes, ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour compter le nombre d'artistes
        query = """
        SELECT COUNT(*) AS total_artists
        FROM tArtist
        """

        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            return None

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de la requête: {e}")
        return None
    finally:
        db.close()


def main():
    """Fonction principale pour l'exercice 2."""
    print("=" * 60)
    print("Exercice 2: Comptage du nombre d'artistes")
    print("=" * 60)
    print()

    # Compter les artistes
    total = count_artists()

    if total is not None:
        print(f"✅ Nombre total d'artistes dans la base de données: {total:,}")
        print()
        print(f"📊 Explication de la requête:")
        print("   La requête SQL utilise COUNT(*) pour compter toutes les lignes")
        print("   de la table tArtist, qui contient l'ensemble des artistes.")
    else:
        print("❌ Impossible de compter les artistes.")

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
