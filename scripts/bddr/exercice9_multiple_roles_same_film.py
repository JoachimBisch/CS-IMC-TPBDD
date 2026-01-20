import sys
sys.path.insert(0, '/workspaces/CS-IMC-TPBDD/scripts')

from db_connector import DatabaseConnector


def get_artists_with_multiple_roles_in_film():
    db = DatabaseConnector()
    conn = db.get_sql_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        a.idArtist,
        a.primaryName,
        j.idFilm,
        f.primaryTitle,
        COUNT(DISTINCT j.category) AS role_count
    FROM tArtist a
    INNER JOIN tJob j ON a.idArtist = j.idArtist
    INNER JOIN tFilm f ON j.idFilm = f.idFilm
    GROUP BY a.idArtist, a.primaryName, j.idFilm, f.primaryTitle
    HAVING COUNT(DISTINCT j.category) > 1
    ORDER BY a.primaryName, f.primaryTitle
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()
    
    return results


def get_roles_for_artist_and_film(artist_id, film_id):
    db = DatabaseConnector()
    conn = db.get_sql_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT category
    FROM tJob
    WHERE idArtist = ? AND idFilm = ?
    ORDER BY category
    """
    
    cursor.execute(query, (artist_id, film_id))
    results = cursor.fetchall()
    db.close()
    
    return [row[0] for row in results]


def main():
    results = get_artists_with_multiple_roles_in_film()
    
    if not results:
        print("Aucun artiste ayant plusieurs responsabilités dans un même film")
        return
    
    print(f"Nombre total de combinaisons artiste-film avec plusieurs responsabilités: {len(results)}\n")
    print("=" * 100)
    print(f"{'Artiste':<40} {'Film':<50} {'Responsabilités':<20}")
    print("=" * 100)
    
    current_artist_film = None
    for row in results:
        artist_id, artist_name, film_id, film_title, role_count = row
        
        roles = get_roles_for_artist_and_film(artist_id, film_id)
        roles_str = ", ".join(roles)
        
        artist_disp = artist_name[:37] + "..." if len(artist_name) > 40 else artist_name
        film_disp = film_title[:47] + "..." if len(film_title) > 50 else film_title
        
        print(f"{artist_disp:<40} {film_disp:<50} {roles_str:<20}")
    
    print("=" * 100)


if __name__ == "__main__":
    main()
