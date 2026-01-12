"""
Exercice 4: Trouver l'année de naissance la plus représentée parmi les acteurs (sauf 0)
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def get_most_common_birth_year():
    """
    Trouve l'année de naissance la plus représentée parmi les artistes (excluant 0).

    Returns:
        tuple: (année, nombre d'artistes), ou (None, None) en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour trouver l'année de naissance la plus représentée
        query = """
        SELECT TOP 1 birthYear, COUNT(*) AS artist_count
        FROM tArtist
        WHERE birthYear <> 0 AND birthYear IS NOT NULL
        GROUP BY birthYear
        ORDER BY artist_count DESC
        """

        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            return result[0], result[1]
        else:
            return None, None

    except Exception as e:
        print(f"❌ Erreur lors de l'exécution de la requête: {e}")
        return None, None
    finally:
        db.close()


def get_top_birth_years(limit=10):
    """
    Récupère les années de naissance les plus représentées.

    Args:
        limit (int): Nombre d'années à récupérer

    Returns:
        list: Liste de tuples (année, nombre), ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        query = f"""
        SELECT TOP {limit} birthYear, COUNT(*) AS artist_count
        FROM tArtist
        WHERE birthYear <> 0 AND birthYear IS NOT NULL
        GROUP BY birthYear
        ORDER BY artist_count DESC
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
    """Fonction principale pour l'exercice 4."""
    print("=" * 60)
    print("Exercice 4: Année de naissance la plus représentée")
    print("=" * 60)
    print()

    # Trouver l'année la plus représentée
    birth_year, count = get_most_common_birth_year()

    if birth_year is not None and count is not None:
        print(f"✅ Année de naissance la plus représentée: {birth_year}")
        print(f"👥 Nombre d'artistes nés cette année: {count:,}")
        print()
        
        # Afficher le top 10 pour contexte
        print("📊 Top 10 des années de naissance les plus représentées:")
        print("-" * 60)
        
        top_years = get_top_birth_years(10)
        if top_years:
            for i, (year, artist_count) in enumerate(top_years, 1):
                bar = "█" * min(50, int(artist_count / top_years[0][1] * 50))
                print(f"  {i:2d}. {year}: {artist_count:,} artistes {bar}")
        
        print()
        print(f"📝 Explication de la requête:")
        print(f"   La requête SQL utilise GROUP BY birthYear pour regrouper")
        print(f"   les artistes par année de naissance, COUNT(*) pour compter")
        print(f"   le nombre d'artistes par année, WHERE birthYear <> 0 pour")
        print(f"   exclure les années invalides, ORDER BY COUNT DESC pour trier")
        print(f"   du plus grand au plus petit, et TOP 1 pour ne garder que")
        print(f"   l'année la plus représentée.")
    else:
        print("❌ Impossible de trouver l'année de naissance la plus représentée.")

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
