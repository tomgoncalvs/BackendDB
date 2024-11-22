from flask import Blueprint, jsonify, request, current_app
from app.dboracle import get_oracle_connection
from app.mongodb import get_mongo_client
import json
from datetime import datetime
import uuid

routes = Blueprint("routes", __name__)

def log_message(message):
    """Função para centralizar logs no console."""
    current_app.logger.info(message)

@routes.route('/migrate', methods=['POST'])
def migrate_data():
    """Rota para migrar uma tabela específica do Oracle para o MongoDB."""
    try:
        data = request.get_json()
        table_name = data.get("table_name")
        if not table_name:
            return jsonify({"error": "Table name is required"}), 400

        log_message(f"Starting migration for table: {table_name}")

        # Conectar ao Oracle
        oracle_conn = get_oracle_connection()
        cursor = oracle_conn.cursor()

        # Executar procedure no Oracle
        cursor.callproc("ExportTableToJSON", [table_name])
        cursor.execute(
            "SELECT json_data FROM json_export_log WHERE table_name = :1 ORDER BY export_date DESC FETCH FIRST 1 ROW ONLY",
            [table_name]
        )
        result = cursor.fetchone()

        if not result:
            log_message(f"No data found for table {table_name}")
            return jsonify({"error": f"No data found for table {table_name}"}), 404

        # Converter o CLOB para string
        json_clob = result[0]
        json_data = json_clob.read() if hasattr(json_clob, 'read') else str(json_clob)

        # Validar e converter JSON
        try:
            json_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            log_message(f"Error decoding JSON for table {table_name}: {str(e)}")
            return jsonify({"error": f"Error parsing JSON: {str(e)}"}), 500

        # Conectar ao MongoDB
        mongo_db = get_mongo_client()
        collection = mongo_db["oracle_energy"]

        # Inserir dados
        new_data = []
        for record in json_data:
            # Gerar _id único
            unique_id = record.get("fornecedor_id") or record.get("cliente_id") or \
                        record.get("energia_id") or record.get("estoque_id") or \
                        record.get("transacao_id") or record.get("audit_id")
            if not unique_id:
                unique_id = str(uuid.uuid4())
            record["_id"] = f"{table_name}_{unique_id}"
            record["table_name"] = table_name

            # Verificar duplicidade
            if not collection.find_one({"_id": record["_id"]}):
                new_data.append(record)

        if new_data:
            collection.insert_many(new_data)
            log_message(f"Inserted {len(new_data)} records into collection oracle_energy for table {table_name}")
        else:
            log_message(f"No new records to insert for table {table_name}")

        # Fechar conexões
        cursor.close()
        oracle_conn.close()

        return jsonify({
            "message": f"Migração da tabela {table_name} concluída.",
            "exported_records": len(json_data),
            "inserted_records": len(new_data),
        })

    except Exception as e:
        log_message(f"Error during migration: {str(e)}")
        return jsonify({"error": str(e)}), 500

@routes.route('/migrate_all', methods=['POST'])
def migrate_all_tables():
    """Rota para migrar todas as tabelas do Oracle para o MongoDB."""
    try:
        tables_to_migrate = [
            "tb_app_fornecedores",
            "tb_app_clientes",
            "tb_app_energia",
            "tb_app_estoqueEnergia",
            "tb_app_transacao",
            "tb_audit_log"
        ]

        migration_results = []

        oracle_conn = get_oracle_connection()
        cursor = oracle_conn.cursor()
        mongo_db = get_mongo_client()
        collection = mongo_db["oracle_energy"]

        for table_name in tables_to_migrate:
            try:
                log_message(f"Starting migration for table: {table_name}")

                # Executar procedure no Oracle
                cursor.callproc("ExportTableToJSON", [table_name])
                cursor.execute(
                    "SELECT json_data FROM json_export_log WHERE table_name = :1 ORDER BY export_date DESC FETCH FIRST 1 ROW ONLY",
                    [table_name]
                )
                result = cursor.fetchone()

                if not result:
                    log_message(f"No data found for table {table_name}")
                    migration_results.append({
                        "table_name": table_name,
                        "status": "No data found",
                        "exported_records": 0,
                        "inserted_records": 0
                    })
                    continue

                # Converter o CLOB para string
                json_clob = result[0]
                json_data = json_clob.read() if hasattr(json_clob, 'read') else str(json_clob)

                # Validar e converter JSON
                try:
                    json_data = json.loads(json_data)
                except json.JSONDecodeError as e:
                    log_message(f"Error parsing JSON for table {table_name}: {str(e)}")
                    migration_results.append({
                        "table_name": table_name,
                        "status": f"Error parsing JSON: {str(e)}",
                        "exported_records": 0,
                        "inserted_records": 0
                    })
                    continue

                # Inserir dados
                new_data = []
                for record in json_data:
                    # Gerar _id único
                    unique_id = record.get("fornecedor_id") or record.get("cliente_id") or \
                                record.get("energia_id") or record.get("estoque_id") or \
                                record.get("transacao_id") or record.get("audit_id")
                    if not unique_id:
                        unique_id = str(uuid.uuid4())
                    record["_id"] = f"{table_name}_{unique_id}"
                    record["table_name"] = table_name

                    # Verificar duplicidade
                    if not collection.find_one({"_id": record["_id"]}):
                        new_data.append(record)

                if new_data:
                    collection.insert_many(new_data)
                    log_message(f"Inserted {len(new_data)} records into collection oracle_energy for table {table_name}")
                else:
                    log_message(f"No new records to insert for table {table_name}")

                migration_results.append({
                    "table_name": table_name,
                    "status": "Success",
                    "exported_records": len(json_data),
                    "inserted_records": len(new_data)
                })

            except Exception as table_error:
                log_message(f"Error during migration for table {table_name}: {str(table_error)}")
                migration_results.append({
                    "table_name": table_name,
                    "status": f"Error: {str(table_error)}",
                    "exported_records": 0,
                    "inserted_records": 0
                })

        # Fechar conexões
        cursor.close()
        oracle_conn.close()

        return jsonify({
            "message": "Migração de todas as tabelas concluída.",
            "results": migration_results
        })

    except Exception as e:
        log_message(f"Error during migration of all tables: {str(e)}")
        return jsonify({"error": str(e)}), 500
