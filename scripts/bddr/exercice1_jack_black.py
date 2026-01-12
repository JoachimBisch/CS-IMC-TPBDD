"""
Exercice 1: Visualiser l'année de naissance de Jack Black
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent  ))

from db_connector import DatabaseConnector


def get_artist_birth_year(artist_name):
    """
    Récupère l'année de naissance d'un artiste.

    Args:
        artist_name (str): Nom de l'artiste

    Returns:
        int: Année de naissance de l'artiste, ou None si non trouvé
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour obtenir l'année de naissance
        query = """
        SELECT primaryName, birthYear
        FROM tArtist
        WHERE primaryName = 'Jack Black'
        """

        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            name, birth_year = result
            return name, birth_year
        else:
            return None, None

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de la requête: {e}")
        return None, None
    finally:
        db.close()


def main():
    """Fonction principale pour l'exercice 1."""
    print("=" * 60)
    print("Exercice 1: Année de naissance de Jack Black")
    print("=" * 60)
    print()

    # Rechercher Jack Black
    name, birth_year = get_artist_birth_year("Jack Black")

    if name and birth_year:
        print(f"✅ Artiste trouvé: {name}")
        print(f"📅 Année de naissance: {birth_year}")
    else:
        print("❌ L'artiste 'Jack Black' n'a pas été trouvé dans la base de données.")
        print()
        print("Recherche des artistes contenant 'Black'...")

        # Si Jack Black n'est pas trouvé, chercher les artistes similaires
        db = DatabaseConnector()
        try:
            conn = db.get_sql_connection()
            cursor = conn.cursor()

            query = """
            SELECT primaryName, birthYear
            FROM tArtist
            WHERE primaryName LIKE ?
            ORDER BY primaryName
            """

            cursor.execute(query, ("%Black%",))
            results = cursor.fetchall()

            if results:
                print(f"\n🔍 {len(results)} artiste(s) trouvé(s) contenant 'Black':")
                print("-" * 60)
                for artist_name, artist_birth_year in results:
                    year_str = str(artist_birth_year) if artist_birth_year else "N/A"
                    print(f"  • {artist_name}: {year_str}")
            else:
                print("Aucun artiste contenant 'Black' trouvé.")

        except Exception as e:
            print(f"❌ Erreur: {e}")
        finally:
            db.close()

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
