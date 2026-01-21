"""
Exercice : Ajouter des réalisateurs au film dans Neo4j.
"""

import sys
from pathlib import Path

# Ajouter le répertoire scripts au chemin pour importer db_connector
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_connector import DatabaseConnector


def add_and_verify_directors():
    """Ajoute des réalisateurs au film et vérifie leur création."""
    
    # Initialiser la connexion
    db = DatabaseConnector()
    graph = db.get_neo4j_graph()
    
    # Données
    movie_title = "L'histoire de mon 20 au cours Infrastructure de donnees"
    directors = [
        {"firstName": "Laurent", "lastName": "Cabaret"},
        {"firstName": "Philippe", "lastName": "Carlamar"}
    ]
    
    print(f"🎬 Film : {movie_title}")
    print("-" * 70)
    print()
    
    # Créer les nœuds réalisateurs et les relations DIRECTED
    for director in directors:
        first_name = director["firstName"]
        last_name = director["lastName"]
        
        print(f"🔄 Ajout du réalisateur : {first_name} {last_name}")
        
        # Créer ou récupérer le nœud Person et ajouter la relation DIRECTED
        query_create_director = """
        MERGE (p:Person {firstName: $first_name, lastName: $last_name})
        SET p.name = $first_name + ' ' + $last_name
        WITH p
        MATCH (m:Movie {title: $movie_title})
        MERGE (p)-[r:DIRECTED]->(m)
        RETURN p, r, m
        """
        
        try:
            result = graph.run(
                query_create_director,
                first_name=first_name,
                last_name=last_name,
                movie_title=movie_title
            )
            records = result.data()
            
            if records:
                print("   ✅ Réalisateur ajouté et relation DIRECTED créée !")
                record = records[0]
                person = record['p']
                print(f"      - Nom complet : {person['name']}")
            else:
                print("   ⚠️ Pas de résultat retourné")
                
        except Exception as e:
            print(f"   ❌ Erreur : {e}")
    
    print()
    print("🔍 Vérification des réalisateurs...")
    print("-" * 70)
    
    # Vérifier les relations DIRECTED
    query_verify = """
    MATCH (p:Person)-[r:DIRECTED]->(m:Movie {title: $movie_title})
    RETURN p.name AS director_name, type(r) AS relation_type
    """
    
    try:
        result = graph.run(query_verify, movie_title=movie_title)
        records = result.data()
        
        if records:
            print("✅ Réalisateurs trouvés dans le graphe !")
            for record in records:
                print(f"   - {record['director_name']} ({record['relation_type']})")
        else:
            print("❌ Aucun réalisateur trouvé")
            
    except Exception as e:
        print(f"❌ Erreur lors de la vérification : {e}")
    
    print()
    print("📊 Statistiques du graphe")
    print("-" * 70)
    
    # Afficher les statistiques
    query_stats = """
    MATCH (p:Person)-[r:DIRECTED]->(m:Movie)
    RETURN COUNT(r) AS total_directed
    """
    
    try:
        result = graph.run(query_stats)
        records = result.data()
        
        if records:
            total = records[0]['total_directed']
            print(f"✓ Nombre total de relations DIRECTED : {total}")
            
    except Exception as e:
        print(f"❌ Erreur : {e}")
    
    # Afficher un résumé complet du film
    print()
    print(f"🎬 Résumé du film '{movie_title}' :")
    print("-" * 70)
    
    query_summary = """
    MATCH (m:Movie {title: $movie_title})
    OPTIONAL MATCH (director:Person)-[:DIRECTED]->(m)
    OPTIONAL MATCH (actor:Person)-[:ACTED_IN]->(m)
    RETURN m.title AS movie_title, 
           COLLECT(DISTINCT director.name) AS directors,
           COLLECT(DISTINCT actor.name) AS actors
    """
    
    try:
        result = graph.run(query_summary, movie_title=movie_title)
        records = result.data()
        
        if records:
            record = records[0]
            print(f"Titre : {record['movie_title']}")
            
            directors_list = [d for d in record['directors'] if d is not None]
            print(f"Réalisateurs ({len(directors_list)}) :")
            if directors_list:
                for director in directors_list:
                    print(f"   - {director}")
            else:
                print("   Aucun réalisateur")
            
            actors_list = [a for a in record['actors'] if a is not None]
            print(f"Acteurs ({len(actors_list)}) :")
            if actors_list:
                for actor in actors_list:
                    print(f"   - {actor}")
            else:
                print("   Aucun acteur")
                
    except Exception as e:
        print(f"❌ Erreur : {e}")


if __name__ == "__main__":
    add_and_verify_directors()
