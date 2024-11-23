import os
from flask import Blueprint, jsonify, request, current_app, send_file
from app.dboracle import get_oracle_connection
from app.mongodb import get_mongo_client
import json
from datetime import datetime
import uuid

routes = Blueprint("routes", __name__)

EXPORT_DIR = "exports"

def log_message(message):
    """Função para centralizar logs no console."""
    current_app.logger.info(message)

def save_json_to_file(data, file_name):
    """Salva os dados JSON em um arquivo no diretório de exportação."""
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)

    file_path = os.path.join(EXPORT_DIR, file_name)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return file_path

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
        collection = mongo_db[table_name]  # Usar o nome da tabela como nome da coleção

        # Inserir dados
        for record in json_data:
            # Usar o identificador único da tabela como _id
            unique_id = record.get("fornecedor_id") or record.get("cliente_id") or \
                        record.get("energia_id") or record.get("estoque_id") or \
                        record.get("transacao_id") or record.get("audit_id")
            if unique_id:
                record["_id"] = unique_id  # Usar o ID original como _id
            else:
                record["_id"] = str(uuid.uuid4())  # Gerar um UUID se não existir ID

            # Inserir ou atualizar (upsert) o registro
            collection.update_one({"_id": record["_id"]}, {"$set": record}, upsert=True)

        log_message(f"Records processed for table {table_name}")

        # Salvar os dados exportados como JSON
        file_path = save_json_to_file(json_data, f"{table_name}.json")

        # Fechar conexões
        cursor.close()
        oracle_conn.close()

        return send_file(file_path, as_attachment=True)

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

        migration_results = {}
        all_data = {}

        oracle_conn = get_oracle_connection()
        cursor = oracle_conn.cursor()
        mongo_db = get_mongo_client()  # Conectar ao banco de dados oracle_energy

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
                    migration_results[table_name] = {
                        "status": "No data found",
                        "exported_records": 0,
                        "inserted_records": 0
                    }
                    continue

                # Converter o CLOB para string
                json_clob = result[0]
                json_data = json_clob.read() if hasattr(json_clob, 'read') else str(json_clob)

                # Validar e converter JSON
                try:
                    json_data = json.loads(json_data)
                except json.JSONDecodeError as e:
                    log_message(f"Error parsing JSON for table {table_name}: {str(e)}")
                    migration_results[table_name] = {
                        "status": f"Error parsing JSON: {str(e)}",
                        "exported_records": 0,
                        "inserted_records": 0
                    }
                    continue

                # Conectar à coleção específica
                collection = mongo_db[table_name]

                # Inserir dados
                for record in json_data:
                    unique_id = record.get("fornecedor_id") or record.get("cliente_id") or \
                                record.get("energia_id") or record.get("estoque_id") or \
                                record.get("transacao_id") or record.get("audit_id")
                    if unique_id:
                        record["_id"] = unique_id  # Usar o ID original como _id
                    else:
                        record["_id"] = str(uuid.uuid4())
                    collection.update_one({"_id": record["_id"]}, {"$set": record}, upsert=True)

                all_data[table_name] = json_data
                migration_results[table_name] = {
                    "status": "Success",
                    "exported_records": len(json_data),
                }

            except Exception as table_error:
                log_message(f"Error during migration for table {table_name}: {str(table_error)}")
                migration_results[table_name] = {
                    "status": f"Error: {str(table_error)}",
                    "exported_records": 0,
                }

        # Salvar todos os dados exportados como JSON
        file_path = save_json_to_file(all_data, "all_tables.json")

        # Fechar conexões
        cursor.close()
        oracle_conn.close()

        return send_file(file_path, as_attachment=True)

    except Exception as e:
        log_message(f"Error during migration of all tables: {str(e)}")
        return jsonify({"error": str(e)}), 500
