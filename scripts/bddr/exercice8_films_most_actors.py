"""
Exercice 8: Trouver le nom du ou des film(s) ayant le plus d'acteurs (seulement acted in)
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def get_films_with_actor_count():
    """
    Récupère tous les films avec le nombre d'acteurs (only acted in).

    Returns:
        list: Liste de tuples (idFilm, titre_film, nombre_acteurs), ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour compter les acteurs par film
        query = """
        SELECT 
            f.idFilm,
            f.primaryTitle,
            COUNT(DISTINCT j.idArtist) AS actor_count
        FROM tFilm f
        INNER JOIN tJob j ON f.idFilm = j.idFilm
        WHERE j.category = 'acted in'
        GROUP BY f.idFilm, f.primaryTitle
        ORDER BY actor_count DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        return results

    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête: {e}")
        return None
    finally:
        db.close()


def get_film_details(film_id):
    """
    Récupère les détails d'un film spécifique.

    Args:
        film_id (str): ID du film

    Returns:
        dict: Dictionnaire contenant les détails du film
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            idFilm,
            primaryTitle,
            startYear,
            runtimeMinutes
        FROM tFilm
        WHERE idFilm = ?
        """

        cursor.execute(query, film_id)
        result = cursor.fetchone()

        if result:
            return {
                'idFilm': result[0],
                'titre': result[1],
                'annee': result[2],
                'duree': result[3]
            }
        return None

    except Exception as e:
        return None
    finally:
        db.close()


def main():
    """Fonction principale pour l'exercice 8."""
    print("=" * 80)
    print("Exercice 8: Film(s) ayant le plus d'acteurs")
    print("=" * 80)
    print()

    # Récupérer tous les films avec leur nombre d'acteurs
    results = get_films_with_actor_count()

    if results is not None and len(results) > 0:
        # Trouver le nombre maximum d'acteurs
        max_actor_count = results[0][2]
        
        # Trouver tous les films avec le nombre maximum d'acteurs
        films_with_max_actors = [row for row in results if row[2] == max_actor_count]
        
        print(f"Nombre maximum d'acteurs dans un film: {max_actor_count}")
        print()
        print(f"Film(s) ayant {max_actor_count} acteur(s):")
        print("-" * 80)
        print()
        
        for i, row in enumerate(films_with_max_actors, 1):
            film_id = row[0]
            titre = row[1]
            actor_count = row[2]
            
            # Récupérer les détails du film
            details = get_film_details(film_id)
            
            print(f"{i}. {titre}")
            print(f"   ID Film: {film_id}")
            if details:
                if details['annee']:
                    print(f"   Année: {details['annee']}")
                if details['duree']:
                    print(f"   Durée: {details['duree']} minutes")
            print(f"   Nombre d'acteurs: {actor_count}")
            print()
        
        # Afficher les top 10 films par nombre d'acteurs
        print("-" * 80)
        print("Top 10 des films avec le plus d'acteurs:")
        print("-" * 80)
        print()
        print(f"{'Rang':<5} {'Titre du film':<50} {'Acteurs':<10}")
        print(f"{'-' * 5} {'-' * 50} {'-' * 10}")
        
        for i, row in enumerate(results[:10], 1):
            titre = row[1]
            actor_count = row[2]
            
            # Limiter la longueur du titre si trop long
            if len(titre) > 50:
                titre_display = titre[:47] + "..."
            else:
                titre_display = titre
            
            print(f"{i:<5} {titre_display:<50} {actor_count:<10}")
        
        print()
        print("Explication de la requête:")
        print("  La requête fait une jointure entre tFilm et tJob")
        print("  pour lier les films à leurs acteurs. WHERE category = 'acted in'")
        print("  filtre pour ne garder que les acteurs (pas producteurs, réalisateurs, etc.).")
        print("  GROUP BY regroupe par film, COUNT(DISTINCT j.idArtist) compte")
        print("  le nombre d'acteurs distincts, et ORDER BY trie par nombre")
        print("  d'acteurs décroissant.")
    else:
        print("Impossible de récupérer les films.")

    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
