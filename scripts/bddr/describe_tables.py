"""
Script pour décrire toutes les tables et leurs attributs de la base de données SQL Server.
Répond à l'Exercice 0 du TP.
"""

import sys
from pathlib import Path

# Ajouter le chemin parent pour importer db_connector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent  ))

from db_connector import DatabaseConnector


def describe_tables():
    """Affiche toutes les tables et leurs attributs."""
    
    db = DatabaseConnector()
    
    try:
        # Connexion à SQL Server
        conn = db.get_sql_connection()
        cursor = conn.cursor()
        
        print("\n" + "=" * 80)
        print("DESCRIPTION DES TABLES ET ATTRIBUTS")
        print("=" * 80 + "\n")
        
        # Récupérer la liste des tables
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        tables = cursor.fetchall()
        
        if not tables:
            print("❌ Aucune table trouvée dans la base de données.")
            return
        
        print(f"📊 Nombre total de tables: {len(tables)}\n")
        
        # Pour chaque table, afficher ses colonnes
        for (table_name,) in tables:
            print(f"\n{'─' * 80}")
            print(f"📋 TABLE: {table_name}")
            print(f"{'─' * 80}")
            
            # Récupérer les colonnes de la table
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            columns = cursor.fetchall()
            
            # En-tête du tableau
            print(f"\n{'Colonne':<30} {'Type':<20} {'Nullable':<10} {'Défaut':<15}")
            print(f"{'-' * 30} {'-' * 20} {'-' * 10} {'-' * 15}")
            
            # Afficher chaque colonne
            for col in columns:
                col_name = col[0]
                data_type = col[1]
                max_length = col[2]
                is_nullable = "OUI" if col[3] == "YES" else "NON"
                default_value = col[4] if col[4] else ""
                
                # Formater le type de données avec la longueur si applicable
                if max_length and max_length > 0:
                    type_str = f"{data_type}({max_length})"
                else:
                    type_str = data_type
                
                # Limiter la longueur de la valeur par défaut
                if len(default_value) > 15:
                    default_value = default_value[:12] + "..."
                
                print(f"{col_name:<30} {type_str:<20} {is_nullable:<10} {default_value:<15}")
            
            # Récupérer les clés primaires
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_NAME = ? 
                AND CONSTRAINT_NAME LIKE 'PK_%'
                ORDER BY ORDINAL_POSITION
            """, table_name)
            
            primary_keys = cursor.fetchall()
            if primary_keys:
                pk_list = ", ".join([pk[0] for pk in primary_keys])
                print(f"\n🔑 Clé primaire: {pk_list}")
            
            # Récupérer les clés étrangères
            cursor.execute("""
                SELECT 
                    kcu.COLUMN_NAME,
                    ccu.TABLE_NAME AS REFERENCED_TABLE,
                    ccu.COLUMN_NAME AS REFERENCED_COLUMN
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                    ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
                    ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                WHERE kcu.TABLE_NAME = ?
            """, table_name)
            
            foreign_keys = cursor.fetchall()
            if foreign_keys:
                print(f"\n🔗 Clés étrangères:")
                for fk in foreign_keys:
                    print(f"   {fk[0]} → {fk[1]}.{fk[2]}")
            
            # Compter le nombre de lignes
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                row_count = cursor.fetchone()[0]
                print(f"\n📈 Nombre de lignes: {row_count:,}")
            except Exception as e:
                print(f"\n⚠️  Impossible de compter les lignes: {e}")
        
        print("\n" + "=" * 80)
        print("✅ Description terminée")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la description des tables: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()


def main():
    """Fonction principale."""
    describe_tables()


if __name__ == "__main__":
    main()
