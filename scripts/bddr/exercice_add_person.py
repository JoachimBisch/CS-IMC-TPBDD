"""
Exercice : Ajouter une personne au graphe Neo4j et vérifier sa création.
"""

import sys
from pathlib import Path

# Ajouter le répertoire scripts au chemin pour importer db_connector
sys.path.insert(0, str(Path(__file__).parent.parent))

from db_connector import DatabaseConnector


def add_and_verify_person():
    """Ajoute une personne au graphe et vérifie sa création."""
    
    # Initialiser la connexion
    db = DatabaseConnector()
    graph = db.get_neo4j_graph()
    
    # Personne à ajouter
    first_name = "Joachim"
    last_name = "Bisch Peuchet"
    
    print(f"🔄 Ajout de la personne : {first_name} {last_name}")
    print("-" * 50)
    
    # Créer ou récupérer le nœud Person
    query_create = """
    CREATE (p:Person {name: $first_name + ' ' + $last_name, firstName: $first_name, lastName: $last_name})
    RETURN p
    """
    
    try:
        result = graph.run(query_create, first_name=first_name, last_name=last_name)
        records = result.data()
        
        if records:
            print("✅ Nœud créé avec succès !")
            person = records[0]['p']
            print(f"   - Identifiant Neo4j : {person.identity}")
            print(f"   - Propriétés : {dict(person)}")
        else:
            print("⚠️ Pas de résultat retourné")
            
    except Exception as e:
        print(f"❌ Erreur lors de la création : {e}")
        return
    
    print()
    print("🔍 Vérification de la création...")
    print("-" * 50)
    
    # Vérifier que la personne existe
    query_verify = """
    MATCH (p:Person {firstName: $first_name, lastName: $last_name})
    RETURN p.name AS name, p.firstName AS firstName, p.lastName AS lastName
    """
    
    try:
        result = graph.run(query_verify, first_name=first_name, last_name=last_name)
        records = result.data()
        
        if records:
            print("✅ Personne trouvée dans le graphe !")
            for record in records:
                print(f"   - Nom complet : {record['name']}")
                print(f"   - Prénom : {record['firstName']}")
                print(f"   - Nom : {record['lastName']}")
        else:
            print("❌ Personne non trouvée")
            
    except Exception as e:
        print(f"❌ Erreur lors de la vérification : {e}")
        return
    
    print()
    print("📊 Statistiques du graphe")
    print("-" * 50)
    
    # Afficher le nombre total de nœuds Person
    query_stats = """
    MATCH (p:Person)
    RETURN COUNT(p) AS total
    """
    
    try:
        result = graph.run(query_stats)
        records = result.data()
        
        if records:
            total = records[0]['total']
            print(f"✓ Nombre total de nœuds Person : {total}")
            
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des statistiques : {e}")


if __name__ == "__main__":
    add_and_verify_person()
