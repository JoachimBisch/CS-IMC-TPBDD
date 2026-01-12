"""
Exercice 5: Trouver les artistes ayant joué dans plus d'un film
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def get_artists_with_multiple_films():
    """
    Récupère les artistes ayant joué dans plus d'un film.

    Returns:
        list: Liste de tuples (nom_artiste, nombre_de_films), ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour trouver les artistes ayant joué dans plusieurs films
        query = """
        SELECT a.primaryName, COUNT(DISTINCT p.tconst) AS film_count
        FROM tArtist a
        INNER JOIN tPrincipal p ON a.nconst = p.nconst
        GROUP BY a.nconst, a.primaryName
        HAVING COUNT(DISTINCT p.tconst) > 1
        ORDER BY film_count DESC, a.primaryName
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
    """Fonction principale pour l'exercice 5."""
    print("=" * 60)
    print("Exercice 5: Artistes ayant joué dans plus d'un film")
    print("=" * 60)
    print()

    # Récupérer les artistes ayant joué dans plusieurs films
    results = get_artists_with_multiple_films()

    if results is not None:
        total = len(results)
        
        print(f"✅ Artistes ayant joué dans plus d'un film:")
        print("-" * 60)
        
        # Afficher les premiers artistes (limiter l'affichage si trop nombreux)
        display_limit = 20
        for i, (name, film_count) in enumerate(results[:display_limit]):
            print(f"  {i+1:3d}. {name:40s} - {film_count:,} films")
        
        if total > display_limit:
            print(f"  ... et {total - display_limit:,} autres artistes")
        
        print()
        print(f"📊 Nombre total d'artistes ayant joué dans plus d'un film: {total:,}")
        
        # Statistiques supplémentaires
        if results:
            max_films = results[0][1]
            most_prolific = [name for name, count in results if count == max_films]
            
            print()
            print(f"🏆 Artiste(s) le(s) plus prolifique(s) avec {max_films:,} films:")
            for name in most_prolific[:5]:
                print(f"   • {name}")
            if len(most_prolific) > 5:
                print(f"   ... et {len(most_prolific) - 5} autre(s)")
        
        print()
        print(f"📝 Explication de la requête:")
        print(f"   La requête fait une jointure entre tArtist et tPrincipal")
        print(f"   pour lier les artistes à leurs films. GROUP BY regroupe")
        print(f"   par artiste, COUNT(DISTINCT p.tconst) compte le nombre")
        print(f"   de films distincts, et HAVING COUNT(...) > 1 filtre pour")
        print(f"   ne garder que les artistes ayant joué dans plus d'un film.")
    else:
        print("❌ Impossible de récupérer les artistes.")

    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
