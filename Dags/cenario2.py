from sqlalchemy import create_engine, inspect, text, types

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

# Configuración de las conexiones
database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
cloned_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{cloned_db}'
master_engine = create_engine(database_uri)
cloned_engine = create_engine(cloned_uri)

# Función para obtener tipos de columnas
def get_column_types(engine, table_name, schema):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema)
    column_types = {}
    for col in columns:
        col_type = types.to_instance(col['type'])
        column_types[col['name']] = col_type.compile(dialect=engine.dialect)
    return column_types

main_table = 'db_clone'

columns_main = get_column_types(master_engine, main_table, main_schema)
columns_cloned = get_column_types(cloned_engine, main_table, cloned_schema)

connection = cloned_engine.connect()

transaction = connection.begin()

try:
    for col_name, col_type_main in columns_main.items():
        col_type_cloned = columns_cloned.get(col_name)
        if col_type_cloned and col_type_cloned != col_type_main:
            # Verificar se é necessário ajustar para tipo array
            if '[]' in col_type_main and '[]' not in col_type_cloned:
                alter_query_text = f"""ALTER TABLE {cloned_schema}.{main_table} ALTER COLUMN "{col_name}" TYPE {col_type_main} USING ARRAY["{col_name}"]::{col_type_main}"""
            else:
                alter_query_text = f"""ALTER TABLE {cloned_schema}.{main_table} ALTER COLUMN "{col_name}" TYPE {col_type_main} USING "{col_name}"::{col_type_main}"""
            connection.execute(text(alter_query_text))
            print(f'Column "{col_name}" type changed from {col_type_cloned} to {col_type_main} in {cloned_schema}.{main_table}')

    # Commit apenas se não houve exceções
    transaction.commit()
    print(f"Column types adjusted in {cloned_db}.{cloned_schema}.{main_table}")
except Exception as e:
    # Rollback em caso de exceção
    print(f"Error al ejecutar la consulta: {e}")
    transaction.rollback()
finally:
    connection.close()
    cloned_engine.dispose()
    master_engine.dispose()
