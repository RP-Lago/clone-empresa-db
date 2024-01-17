from sqlalchemy import create_engine, inspect, text, types

"""Cenário 1:  Criar tabela na base de dados clonada
A coluna existe na tabela db_clonee no banco de dados padrão Db_Office, mas não na tabela db_clone no banco de dados clonado Db_OfficeClone."""

# Database connection details
user = "postgres"
password = "postgres"
ip_connection = "localhost"
port = "5433"

# Database and schema details
main_db = "Db_Office"
main_schema = "public"
cloned_db = "Db_OfficeCloned"
cloned_schema = "public"
table_name = "db_clone"

# Configuração das conexões
database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
cloned_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{cloned_db}'
master_engine = create_engine(database_uri)
cloned_engine = create_engine(cloned_uri)

# Create inspectors
inspector = inspect(master_engine)
inspector_clone = inspect(cloned_engine)

# Get table information
main_table = 'db_clone'
cloned_tables = inspector_clone.get_table_names(schema=cloned_schema)

# Get column information
columns_target = inspector_clone.get_columns(main_table, schema=cloned_schema)
columns_target_name = [c['name'] for c in columns_target]
columns_info = inspector.get_columns(main_table, schema=main_schema)

# Exception handling and transaction
try:
    for col in columns_info:
        if col['name'] not in columns_target_name:
            # Create column
            cloned_engine.execute(text(f'ALTER TABLE {cloned_schema}.{main_table} ADD COLUMN "{col["name"]}" {col["type"]}'))
            print(f'Column "{col["name"]}" created in {cloned_schema}.{main_table}')
        else:
            print(f'Column "{col["name"]}" already exists in {cloned_schema}.{main_table}')
except Exception as e:
    print(f'Error: {e}')
else:
    print(f'Column created in {cloned_db}.{cloned_schema}.{main_table}')
finally:
    # Close connection
    cloned_engine.dispose()
    master_engine.dispose()