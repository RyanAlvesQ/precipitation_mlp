from datetime import datetime, timedelta
import numpy as np
from psycopg2.extras import execute_values

class Data:

    def ensure_tables_exist(connection):
        cursor = connection.cursor()
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS mlp_prec;")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS mlp_prec.cptec_precipitacao_municipio (
                    id SERIAL PRIMARY KEY,
                    "CD_MUN" INTEGER NOT NULL,
                    municipio CHARACTER VARYING(100) NOT NULL,
                    estado CHARACTER VARYING(100),
                    precipitacao_acumulada DOUBLE PRECISION NOT NULL,
                    data_arquivo TIMESTAMP,
                    arquivo_original CHARACTER VARYING(255)
                );
            """)

            connection.commit()
        except Exception as error:
            print(f"Erro ao garantir existência das tabelas: {error}")
        finally:
            cursor.close()


    def insert_municipalities_precipitation_data(data, connection, file_name, file_date):
        cursor = connection.cursor()
        try:
            values_to_insert = []
            
            for record in data:
                try:
                    acc = record.get("accumulated_prec")
                    municipality = record["municipality"]
                    municipality_code = record.get("CD_MUN")
                    state = record.get("state").upper() if record.get("state") else None
                    date = record["date"]

                    check_query = """
                        SELECT EXISTS(
                            SELECT 1 FROM mlp_prec.cptec_precipitacao_municipio 
                            WHERE data_arquivo = %s AND estado = %s AND municipio = %s
                        );
                    """
                    cursor.execute(check_query, (date, state, municipality))
                    exists = cursor.fetchone()[0]

                    if not exists:
                        values_to_insert.append((
                            municipality_code, acc,
                            municipality, state, date, file_name
                        ))
                except Exception as e:
                    print(f"Erro ao processar registro {record}: {e}")

            if values_to_insert:
                insert_query = """
                    INSERT INTO mlp_prec.cptec_precipitacao_municipio 
                    ("CD_MUN", precipitacao_acumulada, municipio, estado, data_arquivo, arquivo_original)
                    VALUES %s;
                """
                execute_values(cursor, insert_query, values_to_insert)
                connection.commit()
            
            return {"status": "ok", "message": "Todos os dados foram inseridos com sucesso!"}

        except Exception as error:
            return {"status": "error", "message": f"Erro ao inserir dados no PostgreSQL: {error}"}
        finally:
            cursor.close()


