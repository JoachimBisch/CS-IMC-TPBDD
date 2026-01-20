#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exercice 9: Artistes ayant plusieurs responsabilités dans un même film
Affiche les artistes qui ont eu plusieurs rôles (combinaisons) dans le même film.
Par exemple: acteur ET directeur du même film, etc.
"""

import sys
sys.path.insert(0, '/workspaces/CS-IMC-TPBDD/scripts')

from db_connector import DatabaseConnector


def get_artists_with_multiple_roles_per_film():
    """
    Récupère les artistes qui ont eu plusieurs responsabilités
    (rôles différents) dans le même film.
    """
    db = DatabaseConnector()
    conn = db.get_sql_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        a.idArtist,
        a.primaryName,
        f.idFilm,
        f.primaryTitle,
        COUNT(DISTINCT j.category) as role_count,
        MAX(f.startYear) as year
    FROM tArtist a
    INNER JOIN tJob j ON a.idArtist = j.idArtist
    INNER JOIN tFilm f ON j.idFilm = f.idFilm
    WHERE j.category IS NOT NULL
    GROUP BY a.idArtist, a.primaryName, f.idFilm, f.primaryTitle
    HAVING COUNT(DISTINCT j.category) > 1
    ORDER BY role_count DESC, a.primaryName, f.primaryTitle
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()
    
    return results


def get_roles_for_artist_in_film(artist_id, film_id):
    """
    Récupère les rôles spécifiques d'un artiste dans un film.
    """
    db = DatabaseConnector()
    conn = db.get_sql_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT j.category
    FROM tJob j
    WHERE j.idArtist = ?
    AND j.idFilm = ?
    ORDER BY j.category
    """
    
    cursor.execute(query, (artist_id, film_id))
    results = cursor.fetchall()
    db.close()
    
    return [row[0] for row in results]


def main():
    """
    Affiche les artistes avec plusieurs responsabilités par film.
    """
    results = get_artists_with_multiple_roles_per_film()
    
    if not results:
        print("Aucun artiste n'a eu plusieurs responsabilités dans un même film.")
        return
    
    # Résumé statistique
    total_cases = len(results)
    max_roles = max(row[4] for row in results) if results else 0
    
    print(f"Nombre total de cas: {total_cases}")
    print(f"Nombre maximum de rôles différents: {max_roles}")
    print()
    
    # Affichage par nombre de rôles
    for num_roles in range(max_roles, 0, -1):
        cases = [r for r in results if r[4] == num_roles]
        if cases:
            print(f"\nArtistes avec {num_roles} rôles différents dans un même film:")
            print("-" * 100)
            print(f"{'Artiste':<40} {'Film':<45} {'Rôles':<15}")
            print("-" * 100)
            
            for artist_id, artist_name, film_id, film_title, role_count, year in cases[:20]:
                roles = get_roles_for_artist_in_film(artist_id, film_id)
                roles_str = " + ".join(roles)
                
                # Tronquer les noms trop longs
                artist_display = artist_name[:39] if len(artist_name) > 39 else artist_name
                film_display = film_title[:44] if len(film_title) > 44 else film_title
                
                print(f"{artist_display:<40} {film_display:<45} {roles_str:<15}")
            
            if len(cases) > 20:
                print(f"... et {len(cases) - 20} autres cas avec {num_roles} rôles")
    
    # Statistiques détaillées
    print("\n" + "=" * 100)
    print("STATISTIQUES DÉTAILLÉES")
    print("=" * 100)
    
    # Combinaisons les plus fréquentes
    combination_counts = {}
    for artist_id, artist_name, film_id, film_title, role_count, year in results:
        roles = get_roles_for_artist_in_film(artist_id, film_id)
        combo_key = " + ".join(sorted(roles))
        combination_counts[combo_key] = combination_counts.get(combo_key, 0) + 1
    
    print("\nCombinations de rôles les plus fréquentes:")
    sorted_combos = sorted(combination_counts.items(), key=lambda x: x[1], reverse=True)
    for combo, count in sorted_combos[:10]:
        print(f"  {combo}: {count} cas")


if __name__ == "__main__":
    main()
