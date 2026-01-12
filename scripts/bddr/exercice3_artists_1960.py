"""
Exercice 3: Trouver les noms des artistes nés en 1960 et afficher leur nombre
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def get_artists_by_birth_year():
    """
    Récupère les artistes nés une année donnée.

    Args:
        year (int): Année de naissance

    Returns:
        list: Liste de tuples (nom, année de naissance), ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour trouver les artistes nés en 1960
        query = """
        SELECT primaryName, birthYear
        FROM tArtist
        WHERE birthYear = 1960
        ORDER BY primaryName
        """

        cursor.execute(query)
        results = cursor.fetchall()

        return results

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de la requête: {e}")
        return None
    finally:
        db.close()


def main():
    """Fonction principale pour l'exercice 3."""
    print("=" * 60)
    print("Exercice 3: Artistes nés en 1960")
    print("=" * 60)
    print()

    # Année de recherche
    target_year = 1960

    # Récupérer les artistes nés en 1960
    results = get_artists_by_birth_year()

    if results is not None:
        total = len(results)
        
        print(f"✅ Artistes trouvés nés en {target_year}:")
        print("-" * 60)
        
        # Afficher les premiers artistes (limiter l'affichage si trop nombreux)
        display_limit = 20
        for i, (name, birth_year) in enumerate(results[:display_limit]):
            print(f"  {i+1}. {name} ({birth_year})")
        
        if total > display_limit:
            print(f"  ... et {total - display_limit} autres artistes")
        
        print()
        print(f"📊 Nombre total d'artistes nés en {target_year}: {total:,}")
        print()
        print(f"📝 Explication de la requête:")
        print(f"   La requête SQL utilise WHERE birthYear = {target_year} pour filtrer")
        print(f"   tous les artistes de la table tArtist nés en {target_year},")
        print(f"   puis ORDER BY primaryName pour les trier alphabétiquement.")
    else:
        print(f"❌ Impossible de récupérer les artistes nés en {target_year}.")

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
