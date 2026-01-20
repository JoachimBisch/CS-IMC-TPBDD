"""
Exercice 6: Trouver les artistes ayant eu plusieurs responsabilités au cours de leur carrière
(acteur, directeur, producteur...)
"""

import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_connector import DatabaseConnector


def get_artist_roles(artist_id):
    """
    Récupère les responsabilités d'un artiste spécifique.

    Args:
        artist_id (str): ID de l'artiste

    Returns:
        list: Liste des catégories/responsabilités de l'artiste
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        query = """
        SELECT DISTINCT category
        FROM tJob
        WHERE idArtist = ?
        AND category IS NOT NULL
        ORDER BY category
        """

        cursor.execute(query, artist_id)
        results = cursor.fetchall()

        return [row[0] for row in results]

    except Exception as e:
        return []
    finally:
        db.close()


def get_artists_with_multiple_roles():
    """
    Récupère les artistes ayant eu plusieurs responsabilités différentes.

    Returns:
        list: Liste de tuples (id_artiste, nom_artiste, nombre_de_roles), ou None en cas d'erreur
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête SQL pour trouver les artistes ayant plusieurs responsabilités
        query = """
        SELECT 
            a.idArtist,
            a.primaryName, 
            COUNT(DISTINCT j.category) AS role_count
        FROM tArtist a
        INNER JOIN tJob j ON a.idArtist = j.idArtist
        WHERE j.category IS NOT NULL
        GROUP BY a.idArtist, a.primaryName
        HAVING COUNT(DISTINCT j.category) > 1
        ORDER BY role_count DESC, a.primaryName
        """

        cursor.execute(query)
        results = cursor.fetchall()

        return results

    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête: {e}")
        return None
    finally:
        db.close()


def get_role_statistics():
    """
    Récupère des statistiques sur les responsabilités dans la base de données.

    Returns:
        list: Liste de tuples (role, nombre_d_artistes)
    """
    db = DatabaseConnector()

    try:
        conn = db.get_sql_connection()
        cursor = conn.cursor()

        # Requête pour obtenir les statistiques par rôle
        query = """
        SELECT 
            category,
            COUNT(DISTINCT idArtist) AS artist_count
        FROM tJob
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY artist_count DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        return results

    except Exception as e:
        print(f"Erreur lors de la récupération des statistiques: {e}")
        return None
    finally:
        db.close()


def main():
    """Fonction principale pour l'exercice 6."""
    print("=" * 80)
    print("Exercice 6: Artistes ayant eu plusieurs responsabilités au cours de leur carrière")
    print("=" * 80)
    print()

    print("Statistiques des responsabilités disponibles:")
    print("-" * 80)

    role_stats = get_role_statistics()
    if role_stats:
        print(f"\n{'Responsabilité':<25} {'Nombre d artistes':<20}")
        print(f"{'-' * 25} {'-' * 20}")
        for category, count in role_stats:
            print(f"{category:<25} {count:,}")
        print()

    print()
    print("=" * 80)
    print("Artistes ayant plusieurs responsabilités:")
    print("=" * 80)
    print()

    # Récupérer les artistes ayant plusieurs responsabilités
    results = get_artists_with_multiple_roles()

    if results is not None:
        total = len(results)
        
        print(f"Nombre total d'artistes ayant plusieurs responsabilités: {total:,}")
        print()
        print("-" * 80)
        
        # Afficher les premiers artistes (limiter l'affichage si trop nombreux)
        display_limit = 25
        for i, row in enumerate(results[:display_limit]):
            artist_id = row[0]
            name = row[1]
            role_count = row[2]
            
            # Récupérer les rôles de cet artiste
            roles = get_artist_roles(artist_id)
            roles_str = ", ".join(roles)
            
            print(f"{i+1:3}. {name}")
            print(f"     Responsabilités ({role_count}): {roles_str}")
            print()
        
        if total > display_limit:
            print(f"... et {total - display_limit:,} autres artistes")
        
        print("-" * 80)
        
        # Statistiques supplémentaires
        if results:
            max_roles = results[0][2]
            most_versatile = [row for row in results if row[2] == max_roles]
            
            print()
            print(f"Artistes les plus versatiles (avec {max_roles} responsabilités):")
            for row in most_versatile[:5]:
                artist_id = row[0]
                name = row[1]
                roles = get_artist_roles(artist_id)
                roles_str = ", ".join(roles)
                print(f"  - {name}")
                print(f"    {roles_str}")
            if len(most_versatile) > 5:
                print(f"  ... et {len(most_versatile) - 5} autre(s)")
        
        print()
        print("Explication de la requête:")
        print("  La requête fait une jointure entre tArtist et tJob")
        print("  pour lier les artistes à leurs responsabilités. GROUP BY regroupe")
        print("  par artiste, COUNT(DISTINCT j.category) compte le nombre de")
        print("  responsabilités distinctes, et HAVING COUNT(...) > 1 filtre pour")
        print("  ne garder que les artistes ayant au moins 2 responsabilités")
        print("  différentes.")
    else:
        print("Impossible de récupérer les artistes.")

    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
