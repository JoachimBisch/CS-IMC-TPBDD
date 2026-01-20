"""
Exercice : Ajouter un film au graphe Neo4j et vérifier sa création.
"""

import sys
from pathlib import Path

# Ajouter le répertoire scripts au chemin pour importer db_connector
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_connector import DatabaseConnector


def add_and_verify_movie():
    """Ajoute un film au graphe et vérifie sa création."""
    
    # Initialiser la connexion
    db = DatabaseConnector()
    graph = db.get_neo4j_graph()
    
    # Film à ajouter
    movie_title = "L'histoire de mon 20 au cours Infrastructure de donnees"
    
    print(f"🔄 Ajout du film : {movie_title}")
    print("-" * 60)
    
    # Créer le nœud Movie
    query_create = """
    CREATE (m:Movie {title: $title})
    RETURN m
    """
    
    try:
        result = graph.run(query_create, title=movie_title)
        records = result.data()
        
        if records:
            print("✅ Nœud créé avec succès !")
            movie = records[0]['m']
            print(f"   - Identifiant Neo4j : {movie.identity}")
            print(f"   - Propriétés : {dict(movie)}")
        else:
            print("⚠️ Pas de résultat retourné")
            
    except Exception as e:
        print(f"❌ Erreur lors de la création : {e}")
        return
    
    print()
    print("🔍 Vérification de la création...")
    print("-" * 60)
    
    # Vérifier que le film existe
    query_verify = """
    MATCH (m:Movie {title: $title})
    RETURN m.title AS title
    """
    
    try:
        result = graph.run(query_verify, title=movie_title)
        records = result.data()
        
        if records:
            print("✅ Film trouvé dans le graphe !")
            for record in records:
                print(f"   - Titre : {record['title']}")
        else:
            print("❌ Film non trouvé")
            
    except Exception as e:
        print(f"❌ Erreur lors de la vérification : {e}")
        return
    
    print()
    print("📊 Statistiques du graphe")
    print("-" * 60)
    
    # Afficher le nombre total de nœuds Movie
    query_stats = """
    MATCH (m:Movie)
    RETURN COUNT(m) AS total
    """
    
    try:
        result = graph.run(query_stats)
        records = result.data()
        
        if records:
            total = records[0]['total']
            print(f"✓ Nombre total de nœuds Movie : {total}")
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des statistiques : {e}")


if __name__ == "__main__":
    add_and_verify_movie()
