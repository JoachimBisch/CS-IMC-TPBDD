"""
Exercice : Ajouter une relation ACTED_IN entre une personne et un film dans Neo4j.
"""

import sys
from pathlib import Path

# Ajouter le répertoire scripts au chemin pour importer db_connector
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_connector import DatabaseConnector


def add_and_verify_acted_in_relation():
    """Ajoute une relation ACTED_IN et vérifie sa création."""
    
    # Initialiser la connexion
    db = DatabaseConnector()
    graph = db.get_neo4j_graph()
    
    # Données
    first_name = "Joachim"
    last_name = "Bisch Peuchet"
    movie_title = "L'histoire de mon 20 au cours Infrastructure de donnees"
    
    print(f"🔄 Ajout de la relation ACTED_IN")
    print(f"   Acteur/Actrice : {first_name} {last_name}")
    print(f"   Film : {movie_title}")
    print("-" * 70)
    
    # Créer la relation ACTED_IN entre la personne et le film
    query_create = """
    MATCH (p:Person {firstName: $first_name, lastName: $last_name})
    MATCH (m:Movie {title: $movie_title})
    CREATE (p)-[r:ACTED_IN]->(m)
    RETURN p, r, m
    """
    
    try:
        result = graph.run(query_create, first_name=first_name, last_name=last_name, movie_title=movie_title)
        records = result.data()
        
        if records:
            print("✅ Relation créée avec succès !")
            record = records[0]
            person = record['p']
            relation = record['r']
            movie = record['m']
            print(f"   - Personne : {dict(person)}")
            print(f"   - Relation : {relation.type}")
            print(f"   - Film : {dict(movie)}")
        else:
            print("⚠️ Pas de résultat retourné")
            
    except Exception as e:
        print(f"❌ Erreur lors de la création : {e}")
        return
    
    print()
    print("🔍 Vérification de la relation...")
    print("-" * 70)
    
    # Vérifier que la relation existe
    query_verify = """
    MATCH (p:Person {firstName: $first_name, lastName: $last_name})-[r:ACTED_IN]->(m:Movie {title: $movie_title})
    RETURN p.name AS person_name, type(r) AS relation_type, m.title AS movie_title
    """
    
    try:
        result = graph.run(query_verify, first_name=first_name, last_name=last_name, movie_title=movie_title)
        records = result.data()
        
        if records:
            print("✅ Relation trouvée dans le graphe !")
            for record in records:
                print(f"   - Personne : {record['person_name']}")
                print(f"   - Relation : {record['relation_type']}")
                print(f"   - Film : {record['movie_title']}")
        else:
            print("❌ Relation non trouvée")
            
    except Exception as e:
        print(f"❌ Erreur lors de la vérification : {e}")
        return
    
    print()
    print("📊 Statistiques du graphe")
    print("-" * 70)
    
    # Afficher les relations ACTED_IN
    query_stats = """
    MATCH (p:Person)-[r:ACTED_IN]->(m:Movie)
    RETURN COUNT(r) AS total_relations
    """
    
    try:
        result = graph.run(query_stats)
        records = result.data()
        
        if records:
            total = records[0]['total_relations']
            print(f"✓ Nombre total de relations ACTED_IN : {total}")
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des statistiques : {e}")
    
    # Afficher tous les acteurs du film
    query_actors = """
    MATCH (p:Person)-[r:ACTED_IN]->(m:Movie {title: $movie_title})
    RETURN p.name AS actor_name
    """
    
    try:
        result = graph.run(query_actors, movie_title=movie_title)
        records = result.data()
        
        print()
        print(f"🎬 Acteurs du film '{movie_title}' :")
        if records:
            for record in records:
                print(f"   - {record['actor_name']}")
        else:
            print("   Aucun acteur trouvé")
            
    except Exception as e:
        print(f"❌ Erreur : {e}")


if __name__ == "__main__":
    add_and_verify_acted_in_relation()
