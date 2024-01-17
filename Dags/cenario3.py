from sqlalchemy import create_engine, inspect, text, types
"""Cenário 3: Excluir coluna extra na tabela clonada
Descrição: A coluna existe na tabela db_clone no banco de dados clonado Db_OfficeClone, mas não na tabela db_clonee no banco de dados padrão Db_Office.
Solução: Excluir a coluna extra na tabela db_clone no banco de dados clonado Db_OfficeClone."""

# Detalhes da conexão com o banco de dados
user = "postgres"
password = "postgres"
ip_connection = "localhost"
port = "5433"

# Detalhes do banco de dados e esquema
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

# Função para obter tipos de colunas
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
    # Cenário 3: Excluir coluna extra na tabela clonada
    for col_name, col_type_cloned in columns_cloned.items():
        if col_name not in columns_main:
            alter_query_text = f"""ALTER TABLE {cloned_schema}.{main_table} DROP COLUMN "{col_name}" """
            connection.execute(text(alter_query_text))
            print(f'Column "{col_name}" deleted from {cloned_schema}.{main_table}')

    # Commit apenas se não houve exceções
    transaction.commit()            
    print(f"Column deleted in {cloned_db}.{cloned_schema}.{main_table}")
except Exception as e:
    # Rollback em caso de exceção
    print(f"Error al ejecutar la consulta: {e}")
    transaction.rollback()
finally:
    connection.close()
    cloned_engine.dispose()
    master_engine.dispose()
