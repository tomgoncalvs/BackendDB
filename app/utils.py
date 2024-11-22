import logging
from datetime import datetime
import json

def setup_logger(name="app_logger", log_file="app.log", level=logging.INFO):
    """
    Configura um logger básico para a aplicação.
    
    Args:
        name (str): Nome do logger.
        log_file (str): Caminho do arquivo de log.
        level (int): Nível de log (ex: logging.INFO, logging.DEBUG).

    Returns:
        Logger: Um objeto configurado para logging.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Formatação do log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Handler para arquivo
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def format_date(date_str, input_format="%Y-%m-%d", output_format="%d/%m/%Y"):
    """
    Formata uma data de uma string para outro formato.
    
    Args:
        date_str (str): Data em string no formato de entrada.
        input_format (str): Formato da data de entrada.
        output_format (str): Formato desejado para saída.

    Returns:
        str: Data formatada como string.
    """
    try:
        date_obj = datetime.strptime(date_str, input_format)
        return date_obj.strftime(output_format)
    except ValueError as e:
        raise ValueError(f"Erro ao formatar a data '{date_str}': {e}")

def validate_json(json_data):
    """
    Valida se um dado é um JSON válido.
    
    Args:
        json_data (str): Dados em string para validar.

    Returns:
        bool: True se for JSON válido, False caso contrário.
    """
    try:
        json.loads(json_data)
        return True
    except (TypeError, ValueError):
        return False

def read_json_file(file_path):
    """
    Lê um arquivo JSON e retorna os dados como um dicionário.
    
    Args:
        file_path (str): Caminho do arquivo JSON.

    Returns:
        dict: Dados do arquivo JSON.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Erro ao decodificar JSON do arquivo: {file_path}")

def write_json_file(file_path, data):
    """
    Escreve um dicionário em um arquivo JSON.
    
    Args:
        file_path (str): Caminho para salvar o arquivo.
        data (dict): Dados a serem escritos no arquivo JSON.

    Returns:
        None
    """
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        raise ValueError(f"Erro ao escrever JSON no arquivo '{file_path}': {e}")

def current_timestamp():
    """
    Retorna o timestamp atual em formato ISO 8601.
    
    Returns:
        str: Timestamp atual.
    """
    return datetime.now().isoformat()
