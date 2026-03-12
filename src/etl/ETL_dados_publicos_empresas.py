# -*- coding: utf-8 -*-
"""
ETL para download e processamento dos dados publicos da Receita Federal - CNPJ

Processamento com pandas no host + insercao via copy_records_to_table no staging.
Estrategia de schema swap para manter dados sempre disponiveis.
"""
import argparse
import asyncio
import datetime
import gc
import logging
import pathlib
import socket
from dotenv import load_dotenv
import bs4 as bs
import os
import numpy as np
import pandas as pd
import asyncpg
import re
import sys
import time
import httpx
import zipfile
import concurrent.futures
from rich.console import Console
from rich.progress import (
    Progress, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
)
from rich.logging import RichHandler
from rich.table import Table
import json

# ============================================================
# CONFIGURACAO
# ============================================================

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, rich_tracebacks=True),
        logging.FileHandler("etl_log.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# FUNCOES UTILITARIAS
# ============================================================

async def check_diff(url, file_name, client):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not os.path.isfile(file_name):
        return True

    try:
        response = await client.head(url)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True
    except Exception as e:
        logger.warning(f"Erro ao verificar arquivo {url}: {e}. Assumindo que precisa baixar.")
        return True

    return False


def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def clean_directory(path):
    '''Remove todos os arquivos de um diretorio.'''
    if not os.path.exists(path):
        return 0
    removed = 0
    for filename in os.listdir(path):
        filepath = os.path.join(path, filename)
        if os.path.isfile(filepath):
            os.remove(filepath)
            removed += 1
    return removed


CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'checkpoint.json')

def save_checkpoint(stage, file_index=None):
    checkpoint = {
        'stage': stage,
        'file_index': file_index,
        'timestamp': datetime.datetime.now().isoformat()
    }
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        logger.info(f"Checkpoint salvo: {stage} - arquivo {file_index}")
    except Exception as e:
        logger.error(f"Erro ao salvar checkpoint: {e}")

def load_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
            logger.info(f"Checkpoint carregado: {checkpoint['stage']}")
            return checkpoint
    except Exception as e:
        logger.error(f"Erro ao carregar checkpoint: {e}")
    return None

def clear_checkpoint():
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Checkpoint removido apos conclusao")
    except Exception as e:
        logger.error(f"Erro ao remover checkpoint: {e}")


def getEnv(env, default=None):
    return os.getenv(env, default)


# ============================================================
# CARREGAMENTO DE AMBIENTE
# ============================================================

current_path = pathlib.Path().resolve()
parent_path = current_path.parent
dotenv_path = os.path.join(parent_path, '.env')

if not os.path.isfile(dotenv_path):
    dotenv_path = os.path.join(current_path, '.env')
    if not os.path.isfile(dotenv_path):
        dotenv_path = os.path.join(parent_path, '.env')
        print('Arquivo .env nao encontrado.')
        print(f'Procurando em: {dotenv_path}')

print(f'Carregando configuracoes de: {dotenv_path}')
load_dotenv(dotenv_path=dotenv_path)

parser = argparse.ArgumentParser(description='ETL Dados Publicos CNPJ - Receita Federal')
parser.add_argument('--debug', action='store_true', help='Modo interativo com inputs manuais')
parser.add_argument('--force', action='store_true', help='Forcar execucao mesmo se o mes ja foi processado com sucesso')
cli_args = parser.parse_args()

DEBUG_MODE = cli_args.debug


def get_previous_month():
    """Retorna (ano, mes) do mes anterior ao atual."""
    today = datetime.date.today()
    first_of_month = today.replace(day=1)
    last_month = first_of_month - datetime.timedelta(days=1)
    return last_month.year, last_month.month


def get_year_month():
    """Retorna ano e mes. Em modo debug, solicita input. Senao, usa mes anterior."""
    current_year = datetime.datetime.now().year

    if not DEBUG_MODE:
        year, month = get_previous_month()
        print(f"\n[MODO AUTOMATICO] Usando mes anterior: {year}-{month:02d}")
        return year, month

    print("\n" + "="*50)
    print("CONFIGURACAO DE ANO E MES PARA DOWNLOAD DOS DADOS")
    print("="*50)

    while True:
        try:
            year = input(f"Digite o ano (exemplo: {current_year}): ").strip()
            year = int(year) if year else current_year
            if year < 2019 or year > current_year:
                print(f"Ano deve estar entre 2019 e {current_year}")
                continue
            break
        except ValueError:
            print("Por favor, digite um ano valido")

    current_month = datetime.datetime.now().month
    while True:
        try:
            month = input(f"Digite o mes (1-12, exemplo: {current_month}): ").strip()
            month = int(month) if month else current_month
            if month < 1 or month > 12:
                print("Mes deve estar entre 1 e 12")
                continue
            break
        except ValueError:
            print("Por favor, digite um mes valido")

    return year, month


ano, mes = get_year_month()
mes_formatado = f"{mes:02d}"
print(f"\nConfigurado para baixar dados de: {ano}-{mes_formatado}")
print("="*50)

base_url = "https://arquivos.receitafederal.gov.br/public.php/dav/files/gn672Ad4CF8N6TK"
read_url = f"{base_url}/Dados/Cadastros/CNPJ/{ano}-{mes_formatado}/"

output_files = getEnv('OUTPUT_FILES_PATH')
extracted_files = getEnv('EXTRACTED_FILES_PATH')

if not output_files or not extracted_files:
    logger.error('OUTPUT_FILES_PATH e/ou EXTRACTED_FILES_PATH nao definidos no .env')
    sys.exit(1)

makedirs(output_files)
makedirs(extracted_files)

# Limpeza de diretorios de execucoes anteriores
existing_downloads = [f for f in os.listdir(output_files) if os.path.isfile(os.path.join(output_files, f))]
existing_extracted = [f for f in os.listdir(extracted_files) if os.path.isfile(os.path.join(extracted_files, f))]

if existing_downloads or existing_extracted:
    if not DEBUG_MODE:
        removed_downloads = clean_directory(output_files)
        removed_extracted = clean_directory(extracted_files)
        logger.info(f"Limpeza automatica: {removed_downloads} downloads e {removed_extracted} extraidos removidos")
    else:
        console.print(f"\n[yellow]Encontrados {len(existing_downloads)} download(s) e {len(existing_extracted)} extraido(s) de execucoes anteriores.[/yellow]")
        resposta = input("Deseja limpar os diretorios antes de iniciar? (s/n): ").strip().lower()
        if resposta == 's':
            clean_directory(output_files)
            clean_directory(extracted_files)
            console.print("[green]Diretorios limpos com sucesso.[/green]")

print(f'Diretorios:\n  output_files: {output_files}\n  extracted_files: {extracted_files}')

# Globais populadas por initialize()
token = None
Files = []


# ============================================================
# TOKEN E LISTAGEM DE ARQUIVOS
# ============================================================

def fetch_request_token(max_retries=5):
    """Obtem token de request com retry e backoff exponencial"""
    import ssl
    share_url = "https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK"

    for attempt in range(max_retries):
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            with httpx.Client(
                timeout=httpx.Timeout(60.0, connect=30.0),
                verify=ssl_context,
                follow_redirects=True,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            ) as client:
                response = client.get(share_url)
                response.raise_for_status()
                html = response.text

            match = re.search(r'data-requesttoken="([^"]+)"', html)
            if match:
                return match.group(1)

            match = re.search(r'"requesttoken"\s*:\s*"([^"]+)"', html)
            if match:
                return match.group(1)

            raise RuntimeError("Token de request nao encontrado na pagina")

        except Exception as e:
            wait_time = min(2 ** attempt * 10, 300)
            logger.warning(f"Token: tentativa {attempt + 1}/{max_retries} falhou: {e}. Aguardando {wait_time}s...")
            if attempt == max_retries - 1:
                raise
            time.sleep(wait_time)


def get_html_with_retry(url, request_token, max_retries=5):
    """Faz request PROPFIND com retry"""
    import ssl

    for attempt in range(max_retries):
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            headers = {
                'requesttoken': request_token,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            with httpx.Client(
                headers=headers,
                timeout=httpx.Timeout(60.0, connect=30.0),
                verify=ssl_context,
                follow_redirects=True
            ) as client:
                response = client.request("PROPFIND", url)
                response.raise_for_status()
                return response.content

        except (httpx.ConnectError, httpx.TimeoutException) as e:
            wait_time = min(2 ** attempt * 10, 300)
            logger.warning(f"PROPFIND: tentativa {attempt + 1}/{max_retries} falhou: {e}. Aguardando {wait_time}s...")
            if attempt == max_retries - 1:
                raise
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Erro inesperado ao acessar {url}: {e}")
            raise


def initialize():
    """Obtem token e lista de arquivos do servidor."""
    global token, Files

    print("Obtendo token de request...")
    token = fetch_request_token()
    print("Token obtido com sucesso!")

    raw_html = get_html_with_retry(read_url, token)
    page_items = bs.BeautifulSoup(raw_html, 'xml')
    Files = []

    for tag in page_items.find_all('d:href'):
        url = tag.text
        if url.endswith('.zip'):
            idx = url.find('/Dados')
            if idx != -1:
                Files.append(url[idx:])

    if not Files:
        raise RuntimeError(f"Nenhum arquivo .zip encontrado na URL {read_url}")

    print(f'Arquivos que serao baixados ({len(Files)}):')
    for f in Files:
        print(f'  {f}')


# ============================================================
# CLASSIFICACAO DE ARQUIVOS EXTRAIDOS
# ============================================================

arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []

def classificar_arquivos_extraidos():
    """Classifica os arquivos extraidos por tipo."""
    global arquivos_empresa, arquivos_estabelecimento, arquivos_socios, arquivos_simples
    global arquivos_cnae, arquivos_moti, arquivos_munic, arquivos_natju, arquivos_pais, arquivos_quals

    for lst in [arquivos_empresa, arquivos_estabelecimento, arquivos_socios, arquivos_simples,
                arquivos_cnae, arquivos_moti, arquivos_munic, arquivos_natju, arquivos_pais, arquivos_quals]:
        lst.clear()

    patterns = {
        'EMPRECSV': arquivos_empresa,
        'ESTABELE': arquivos_estabelecimento,
        'SOCIOCSV': arquivos_socios,
        'SIMPLES.': arquivos_simples,
        'CNAECSV': arquivos_cnae,
        'MOTICSV': arquivos_moti,
        'MUNICCSV': arquivos_munic,
        'NATJUCSV': arquivos_natju,
        'PAISCSV': arquivos_pais,
        'QUALSCSV': arquivos_quals,
    }

    for item in os.listdir(extracted_files):
        for pattern, file_list in patterns.items():
            if pattern in item:
                file_list.append(item)
                break

    total = sum(len(lst) for lst in patterns.values())
    print(f'Arquivos classificados: {total} arquivos extraidos encontrados')
    print(f'  Empresa: {len(arquivos_empresa)}, Estabelecimento: {len(arquivos_estabelecimento)}, '
          f'Socios: {len(arquivos_socios)}, Simples: {len(arquivos_simples)}, '
          f'CNAE: {len(arquivos_cnae)}, Outros: {len(arquivos_moti) + len(arquivos_munic) + len(arquivos_natju) + len(arquivos_pais) + len(arquivos_quals)}')


# ============================================================
# DOWNLOAD
# ============================================================

async def download_file_with_resume(url, file_name, client):
    """Download com retry, resume e deteccao de stall."""
    import aiofiles

    STALL_TIMEOUT = 30
    MAX_CONSECUTIVE_FAILURES = 5

    os.makedirs(output_files, exist_ok=True)
    local_file_path = os.path.join(output_files, file_name)

    consecutive_failures = 0

    while consecutive_failures < MAX_CONSECUTIVE_FAILURES:
        bytes_at_start = 0
        downloaded_bytes = 0
        extra_headers = {}
        file_mode = 'wb'

        if os.path.exists(local_file_path):
            downloaded_bytes = os.path.getsize(local_file_path)
            bytes_at_start = downloaded_bytes
            if downloaded_bytes > 0:
                extra_headers['Range'] = f'bytes={downloaded_bytes}-'
                file_mode = 'ab'

        try:
            async with client.stream('GET', url, headers=extra_headers) as response:
                if response.status_code == 416:
                    print(f'\n{file_name} ja baixado integralmente.')
                    return True

                response.raise_for_status()

                if response.status_code == 206:
                    file_mode = 'ab'
                else:
                    file_mode = 'wb'
                    downloaded_bytes = 0
                    bytes_at_start = 0

                total_size = int(response.headers.get('content-length', 0)) + downloaded_bytes
                last_printed_percent = -1
                chunk_iter = response.aiter_bytes(chunk_size=2097152)

                async with aiofiles.open(local_file_path, file_mode) as f:
                    while True:
                        try:
                            chunk = await asyncio.wait_for(
                                chunk_iter.__anext__(), timeout=STALL_TIMEOUT
                            )
                        except StopAsyncIteration:
                            break
                        except asyncio.TimeoutError:
                            raise httpx.ReadTimeout(
                                f'Stall detectado: nenhum dado por {STALL_TIMEOUT}s'
                            )

                        await f.write(chunk)
                        downloaded_bytes += len(chunk)

                        if total_size > 0:
                            percent = int((downloaded_bytes / total_size) * 100)
                            if percent > last_printed_percent:
                                sys.stdout.write(
                                    f'\r{file_name}: {percent}% '
                                    f'[{downloaded_bytes:,}/{total_size:,}] bytes'
                                )
                                sys.stdout.flush()
                                last_printed_percent = percent

            print(f'\n{file_name} baixado com sucesso!')
            return True

        except (httpx.ConnectError, httpx.TimeoutException,
                httpx.RemoteProtocolError, httpx.ReadError,
                ConnectionResetError, OSError) as e:

            made_progress = downloaded_bytes > bytes_at_start
            if made_progress:
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            wait_time = min(2 ** consecutive_failures * 5, 60)
            logger.warning(
                f'{file_name}: conexao caiu em {downloaded_bytes:,} bytes '
                f'(falhas sem progresso: {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}). '
                f'Retomando em {wait_time}s...'
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(f'{file_name}: {MAX_CONSECUTIVE_FAILURES} falhas consecutivas. Desistindo.')
                return False
            await asyncio.sleep(wait_time)

        except Exception as e:
            logger.error(f'Erro inesperado ao baixar {file_name}: {e}')
            return False

    return False


async def download_all_files():
    """Download sequencial dos arquivos com resume.
    Retorna dict com metricas: total, baixados, atualizados (ja existiam), falhas.
    """
    import ssl

    print(f'Iniciando download de {len(Files)} arquivos...')

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    timeout = httpx.Timeout(120.0, read=60.0, connect=60.0)

    downloaded = 0
    up_to_date = 0
    failed = []

    async with httpx.AsyncClient(
        headers={
            'requesttoken': token,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        timeout=timeout,
        verify=ssl_context,
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=1, max_connections=2)
    ) as client:
        for i, file_entry in enumerate(Files):
            basename = os.path.basename(file_entry)
            url = base_url + file_entry
            file_path_local = os.path.join(output_files, basename)

            print(f'\n[{i+1}/{len(Files)}] {basename}')

            needs_download = await check_diff(url, file_path_local, client)
            if not needs_download:
                print(f'{basename} ja existe e esta atualizado.')
                up_to_date += 1
                continue

            ok = await download_file_with_resume(url, basename, client)
            if ok:
                downloaded += 1
            else:
                failed.append(basename)

    successful = downloaded + up_to_date
    print(f'\nDownloads concluidos: {successful}/{len(Files)} arquivos')
    if failed:
        print(f'Falhas: {", ".join(failed)}')

    return {
        'total': len(Files),
        'baixados': downloaded,
        'atualizados': up_to_date,
        'falhas': len(failed),
    }


# ============================================================
# EXTRACAO
# ============================================================

async def extract_all_files():
    """Extrai todos os arquivos ZIP em paralelo usando threading"""
    print(f'Iniciando extracao de {len(Files)} arquivos...')

    def extract_single_file(file_name):
        try:
            basename = os.path.basename(file_name)
            full_path = os.path.join(output_files, basename)
            if not os.path.exists(full_path):
                return f'Arquivo {basename} nao encontrado em {output_files}'

            try:
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    to_extract = []
                    for info in zip_ref.infolist():
                        dest = os.path.join(extracted_files, info.filename)
                        if os.path.exists(dest) and os.path.getsize(dest) == info.file_size:
                            continue
                        to_extract.append(info)

                    if not to_extract:
                        return f'{basename} ja extraido'

                    for info in to_extract:
                        zip_ref.extract(info, extracted_files)

                return f'{basename} extraido ({len(to_extract)} arquivo(s))'
            except zipfile.BadZipFile:
                return f'Arquivo corrompido {basename}: nao e um ZIP valido'
        except Exception as e:
            return f'Erro ao extrair {basename}: {e}'

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_file = {executor.submit(extract_single_file, f): f for f in Files}
        for future in concurrent.futures.as_completed(future_to_file):
            result = future.result()
            print(result)

    print('Extracao concluida!')


# ============================================================
# PREPARACAO DE DADOS (pandas no host)
# ============================================================

DATE_COLUMNS = {
    'estabelecimento': ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial'],
    'socios': ['data_entrada_sociedade'],
    'simples': ['data_opcao_simples', 'data_exclusao_simples', 'data_opcao_mei', 'data_exclusao_mei']
}


def prepare_dataframe(dataframe, table_name):
    '''
    Prepara DataFrame para insercao: converte datas e substitui NaN/NA por None.
    Retorna lista de tuplas pronta para copy_records_to_table.
    '''
    df = dataframe.copy()

    # Conversao vetorizada de colunas de data (YYYYMMDD string -> date)
    date_cols_converted = set()
    if table_name in DATE_COLUMNS:
        for col in DATE_COLUMNS[table_name]:
            if col in df.columns:
                df[col] = df[col].replace({'00000000': None, '0': None, '': None})
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                date_cols_converted.add(col)

    # Identificar tipos de cada coluna para conversao eficiente
    col_types = {}
    for col in df.columns:
        if col in date_cols_converted:
            col_types[col] = 'date'
        elif hasattr(df[col].dtype, 'numpy_dtype') or str(df[col].dtype).startswith('Int'):
            col_types[col] = 'nullable_int'
        elif pd.api.types.is_integer_dtype(df[col]):
            col_types[col] = 'int'
        elif pd.api.types.is_float_dtype(df[col]):
            col_types[col] = 'float'
        else:
            col_types[col] = 'other'

    # Converter para lista de tuplas com tipos Python nativos
    columns = list(df.columns)
    records = []
    for row in df.itertuples(index=False, name=None):
        clean_row = []
        for i, val in enumerate(row):
            col_name = columns[i]
            ct = col_types[col_name]

            if val is None or val is pd.NA or (isinstance(val, float) and np.isnan(val)) or (ct == 'date' and pd.isna(val)):
                clean_row.append(None)
            elif ct == 'date':
                clean_row.append(val.date() if hasattr(val, 'date') else val)
            elif ct in ('nullable_int', 'int'):
                clean_row.append(int(val))
            elif ct == 'float':
                clean_row.append(float(val))
            else:
                clean_row.append(str(val) if not isinstance(val, str) else val)
        records.append(tuple(clean_row))

    return records


async def to_sql_async(dataframe, pool, table_name, schema='staging'):
    '''
    Insere dados usando o protocolo COPY do PostgreSQL via asyncpg.
    Insere no schema especificado (default: staging).
    '''
    total = len(dataframe)
    columns = list(dataframe.columns)
    records = prepare_dataframe(dataframe, table_name)

    logger.info(f'{schema}.{table_name}: inserindo {total:,} registros via COPY...')

    async with pool.acquire() as conn:
        try:
            await asyncio.wait_for(
                conn.copy_records_to_table(
                    table_name,
                    records=records,
                    columns=columns,
                    schema_name=schema,
                    timeout=COPY_TIMEOUT,
                ),
                timeout=COPY_TIMEOUT,
            )
        except asyncio.TimeoutError:
            msg = (f'Timeout ao inserir {total:,} registros em {schema}.{table_name} '
                   f'(limite: {COPY_TIMEOUT}s)')
            logger.error(msg)
            raise TimeoutError(msg)

    logger.info(f'{schema}.{table_name}: {total:,} registros inseridos.')


# ============================================================
# DEFINICOES DE TABELAS (schema swap)
# ============================================================

TYPED_TABLES_DDL = {
    'empresa': """
        CREATE TABLE {schema}.empresa (
            cnpj_basico TEXT NOT NULL,
            razao_social TEXT,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social NUMERIC(15,2),
            porte_empresa INTEGER,
            ente_federativo_responsavel TEXT
        )
    """,
    'estabelecimento': """
        CREATE TABLE {schema}.estabelecimento (
            cnpj_basico TEXT NOT NULL,
            cnpj_ordem TEXT NOT NULL,
            cnpj_dv TEXT NOT NULL,
            identificador_matriz_filial INTEGER,
            nome_fantasia TEXT,
            situacao_cadastral INTEGER,
            data_situacao_cadastral DATE,
            motivo_situacao_cadastral INTEGER,
            nome_cidade_exterior TEXT,
            pais INTEGER,
            data_inicio_atividade DATE,
            cnae_fiscal_principal INTEGER,
            cnae_fiscal_secundaria TEXT,
            tipo_logradouro TEXT,
            logradouro TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            uf TEXT,
            municipio INTEGER,
            ddd_1 TEXT,
            telefone_1 TEXT,
            ddd_2 TEXT,
            telefone_2 TEXT,
            ddd_fax TEXT,
            fax TEXT,
            correio_eletronico TEXT,
            situacao_especial TEXT,
            data_situacao_especial DATE
        )
    """,
    'socios': """
        CREATE TABLE {schema}.socios (
            cnpj_basico TEXT NOT NULL,
            identificador_socio INTEGER,
            nome_socio TEXT,
            cnpj_cpf_socio TEXT,
            qualificacao_socio INTEGER,
            data_entrada_sociedade DATE,
            pais INTEGER,
            representante_legal TEXT,
            nome_representante TEXT,
            qualificacao_representante_legal INTEGER,
            faixa_etaria INTEGER
        )
    """,
    'simples': """
        CREATE TABLE {schema}.simples (
            cnpj_basico TEXT NOT NULL,
            opcao_pelo_simples TEXT,
            data_opcao_simples DATE,
            data_exclusao_simples DATE,
            opcao_mei TEXT,
            data_opcao_mei DATE,
            data_exclusao_mei DATE
        )
    """,
    'cnae': """
        CREATE TABLE {schema}.cnae (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
    'motivo': """
        CREATE TABLE {schema}.motivo (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
    'municipio': """
        CREATE TABLE {schema}.municipio (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
    'natureza': """
        CREATE TABLE {schema}.natureza (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
    'pais': """
        CREATE TABLE {schema}.pais (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
    'qualificacao': """
        CREATE TABLE {schema}.qualificacao (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        )
    """,
}

INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico)',
    'CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico)',
    'CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv)',
    'CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj_basico)',
    'CREATE INDEX IF NOT EXISTS idx_simples_cnpj ON simples(cnpj_basico)',
    'CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimento(situacao_cadastral)',
    'CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimento(municipio)',
]

ALL_TABLES = [
    'empresa', 'estabelecimento', 'socios', 'simples',
    'cnae', 'motivo', 'municipio', 'natureza', 'pais', 'qualificacao'
]


# ============================================================
# FUNCOES DE BANCO DE DADOS
# ============================================================

async def create_database_if_not_exists():
    """Cria o banco de dados se nao existir"""
    user = getEnv('DB_USER')
    passw = getEnv('DB_PASSWORD')
    host = getEnv('DB_HOST')
    port = getEnv('DB_PORT')
    database = getEnv('DB_NAME')
    ssl_mode = getEnv('DB_SSL_MODE', 'disable')

    ssl_config = None
    if ssl_mode.lower() in ['disable', 'false']:
        ssl_config = False
    elif ssl_mode.lower() in ['require', 'true']:
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_config = ssl_context
    else:
        ssl_config = 'prefer'

    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = await asyncpg.connect(
                    user=user, password=passw, database='postgres',
                    host=host, port=port, ssl=ssl_config, timeout=30
                )
                break
            except (ConnectionResetError, asyncpg.exceptions.ConnectionDoesNotExistError) as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Tentativa {attempt + 1}/{max_retries} falhou: {e}")
                await asyncio.sleep(2)

        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", database
        )

        if not exists:
            console.print(f"[yellow]Criando banco de dados: {database}[/yellow]")
            await conn.execute(f'CREATE DATABASE "{database}"')
            console.print(f"[green]Banco de dados '{database}' criado![/green]")
        else:
            console.print(f"[blue]Banco de dados '{database}' ja existe[/blue]")

        await conn.close()

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        raise


async def create_db_pool():
    """Cria pool de conexoes assincronas"""
    user = getEnv('DB_USER')
    passw = getEnv('DB_PASSWORD')
    host = getEnv('DB_HOST')
    port = getEnv('DB_PORT')
    database = getEnv('DB_NAME')
    ssl_mode = getEnv('DB_SSL_MODE', 'disable')

    ssl_config = None
    if ssl_mode.lower() in ['disable', 'false']:
        ssl_config = False
    elif ssl_mode.lower() in ['require', 'true']:
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_config = ssl_context
    else:
        ssl_config = 'prefer'

    async def pool_init(conn):
        """Configura TCP keepalive em cada conexao para detectar banco morto."""
        # Envia keepalive a cada 30s; se 3 probes falharem, conexao morre em ~90s
        raw_socket = conn._transport.get_extra_info('socket')
        if raw_socket is not None:
            raw_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Linux-specific keepalive tuning
            if hasattr(socket, 'TCP_KEEPIDLE'):
                raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, 'TCP_KEEPCNT'):
                raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)

    return await asyncpg.create_pool(
        user=user, password=passw, database=database,
        host=host, port=port, ssl=ssl_config,
        min_size=2, max_size=10,
        command_timeout=3600,
        server_settings={
            'client_encoding': 'utf8',
            'timezone': 'UTC'
        },
        init=pool_init,
    )


async def setup_staging_schema(pool):
    """Cria schema staging com todas as tabelas tipadas"""
    async with pool.acquire() as conn:
        await conn.execute('DROP SCHEMA IF EXISTS staging CASCADE')
        await conn.execute('CREATE SCHEMA staging')

        for table_name, ddl in TYPED_TABLES_DDL.items():
            await conn.execute(ddl.format(schema='staging'))
            logger.info(f'Tabela staging.{table_name} criada')

    console.print("[green]Schema staging criado com todas as tabelas[/green]")


async def swap_to_production(pool):
    """Troca staging -> public tabela por tabela, mantendo backup."""
    console.print("\n[bold yellow]Iniciando swap para producao...[/bold yellow]")

    async with pool.acquire() as conn:
        await conn.execute('CREATE SCHEMA IF NOT EXISTS backup')

        for table_name in ALL_TABLES:
            try:
                staging_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'staging' AND table_name = $1)",
                    table_name
                )
                if not staging_exists:
                    logger.warning(f'staging.{table_name} nao encontrada, pulando swap')
                    continue

                async with conn.transaction():
                    await conn.execute(f'DROP TABLE IF EXISTS backup.{table_name}')

                    public_exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = $1)",
                        table_name
                    )
                    if public_exists:
                        await conn.execute(
                            f'ALTER TABLE public.{table_name} SET SCHEMA backup'
                        )

                    await conn.execute(
                        f'ALTER TABLE staging.{table_name} SET SCHEMA public'
                    )

                logger.info(f'Swap concluido: staging.{table_name} -> public.{table_name}')

            except Exception as e:
                logger.error(f'Erro no swap de {table_name}: {e}')
                raise

    console.print("[green]Swap para producao concluido![/green]")


async def db_watchdog(pool, main_task, interval=60):
    """Monitora conectividade com o banco periodicamente.
    Se o banco ficar inacessivel, cancela a task principal para forcar o retry.
    Com interval=60 e max_failures=5, detecta banco morto em ~5 minutos.
    Usa conexao separada do pool, nao interfere nas operacoes pesadas."""
    consecutive_failures = 0
    max_failures = 5
    while True:
        await asyncio.sleep(interval)
        try:
            async with pool.acquire(timeout=10) as conn:
                await conn.fetchval('SELECT 1')
            consecutive_failures = 0
        except asyncio.CancelledError:
            return
        except Exception as e:
            consecutive_failures += 1
            logger.warning(
                f"Watchdog: banco inacessivel ({consecutive_failures}/{max_failures}): {e}"
            )
            if consecutive_failures >= max_failures:
                msg = f"Banco de dados inacessivel apos {max_failures} verificacoes consecutivas: {e}"
                logger.error(f"Watchdog: {msg}. Cancelando operacao principal.")
                main_task.cancel(msg)
                return


INDEX_TIMEOUT = int(os.getenv('ETL_INDEX_TIMEOUT', '1800'))  # 30 min por indice
COPY_TIMEOUT = int(os.getenv('ETL_COPY_TIMEOUT', '900'))    # 15 min por COPY


async def create_indexes(pool):
    """Cria indices nas tabelas de producao"""
    console.print("\n[bold yellow]Criando indices...[/bold yellow]")

    for i, index_sql in enumerate(INDEXES):
        start = time.time()
        async with pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = '{INDEX_TIMEOUT * 1000}'")
            try:
                await asyncio.wait_for(
                    conn.execute(index_sql),
                    timeout=INDEX_TIMEOUT,
                )
                elapsed = time.time() - start
                logger.info(f'Indice [{i+1}/{len(INDEXES)}] criado em {elapsed:.1f}s')
            except asyncio.TimeoutError:
                elapsed = time.time() - start
                msg = (f'Timeout ao criar indice [{i+1}/{len(INDEXES)}] '
                       f'apos {elapsed:.0f}s (limite: {INDEX_TIMEOUT}s)')
                logger.error(msg)
                raise TimeoutError(msg)
            except Exception as e:
                logger.error(f'Erro ao criar indice [{i+1}/{len(INDEXES)}]: {e}')
                raise

    console.print(f"[green]{len(INDEXES)} indices criados![/green]")


# ============================================================
# PROCESSAMENTO DE TABELAS (pandas + copy_records no staging)
# ============================================================

async def process_empresa_files(pool):
    empresa_start = time.time()
    console.print("\n[bold cyan]Processando EMPRESA...[/bold cyan]")

    empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
    empresa_columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
                        'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

    for e in range(len(arquivos_empresa)):
        logger.info(f'Empresa: {arquivos_empresa[e]}')
        filepath = os.path.join(extracted_files, arquivos_empresa[e])

        try:
            empresa = pd.read_csv(filepath, sep=';', header=None,
                                   dtype=empresa_dtypes, encoding='latin-1')
        except pd.errors.EmptyDataError:
            logger.info(f'Arquivo {arquivos_empresa[e]} vazio. Pulando...')
            continue
        except Exception as ex:
            logger.error(f'Erro ao ler {arquivos_empresa[e]}: {ex}')
            continue

        empresa.columns = empresa_columns
        empresa['capital_social'] = empresa['capital_social'].str.replace(',', '.').astype(float)

        await to_sql_async(empresa, pool, 'empresa')
        del empresa
        gc.collect()

    elapsed = time.time() - empresa_start
    console.print(f"[green]EMPRESA concluida em {elapsed:.1f}s[/green]")


async def process_estabelecimento_files(pool):
    estab_start = time.time()
    console.print("\n[bold cyan]Processando ESTABELECIMENTO...[/bold cyan]")

    estab_dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32',
                    6: object, 7: 'Int32', 8: object, 9: 'Int32', 10: object, 11: 'Int32',
                    12: object, 13: object, 14: object, 15: object, 16: object, 17: object,
                    18: object, 19: object, 20: 'Int32', 21: object, 22: object, 23: object,
                    24: object, 25: object, 26: object, 27: object, 28: object, 29: object}
    estab_columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                     'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                     'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
                     'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                     'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
                     'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                     'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial',
                     'data_situacao_especial']

    CHUNK_SIZE = 500_000

    for e in range(len(arquivos_estabelecimento)):
        logger.info(f'Estabelecimento [{e+1}/{len(arquivos_estabelecimento)}]: {arquivos_estabelecimento[e]}')
        filepath = os.path.join(extracted_files, arquivos_estabelecimento[e])

        part = 0
        while True:
            try:
                df = pd.read_csv(filepath, sep=';', nrows=CHUNK_SIZE,
                                  skiprows=CHUNK_SIZE * part, header=None,
                                  dtype=estab_dtypes, encoding='latin-1')
            except pd.errors.EmptyDataError:
                break
            except Exception as ex:
                logger.error(f'Erro ao ler {arquivos_estabelecimento[e]} parte {part+1}: {ex}')
                break

            if df.empty:
                break

            df.columns = estab_columns
            await to_sql_async(df, pool, 'estabelecimento')
            logger.info(f'Estabelecimento {arquivos_estabelecimento[e]} parte {part+1} inserida ({len(df):,} registros)')

            part += 1
            del df
            gc.collect()

    elapsed = time.time() - estab_start
    console.print(f"[green]ESTABELECIMENTO concluido em {elapsed:.1f}s[/green]")


async def process_socios_files(pool):
    socios_start = time.time()
    console.print("\n[bold cyan]Processando SOCIOS...[/bold cyan]")

    socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32',
                     5: object, 6: 'Int32', 7: object, 8: object, 9: 'Int32', 10: 'Int32'}
    socios_columns = ['cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
                      'qualificacao_socio', 'data_entrada_sociedade', 'pais',
                      'representante_legal', 'nome_representante',
                      'qualificacao_representante_legal', 'faixa_etaria']

    for e in range(len(arquivos_socios)):
        logger.info(f'Socios: {arquivos_socios[e]}')
        filepath = os.path.join(extracted_files, arquivos_socios[e])

        try:
            socios = pd.read_csv(filepath, sep=';', header=None,
                                  dtype=socios_dtypes, encoding='latin-1')
        except pd.errors.EmptyDataError:
            logger.info(f'Arquivo {arquivos_socios[e]} vazio. Pulando...')
            continue
        except Exception as ex:
            logger.error(f'Erro ao ler {arquivos_socios[e]}: {ex}')
            continue

        socios.columns = socios_columns
        await to_sql_async(socios, pool, 'socios')

        del socios
        gc.collect()

    elapsed = time.time() - socios_start
    console.print(f"[green]SOCIOS concluido em {elapsed:.1f}s[/green]")


async def process_simples_files(pool):
    simples_start = time.time()
    console.print("\n[bold cyan]Processando SIMPLES...[/bold cyan]")

    simples_dtypes = {0: object, 1: object, 2: object, 3: object,
                      4: object, 5: object, 6: object}
    simples_columns = ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples',
                        'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei',
                        'data_exclusao_mei']

    for e in range(len(arquivos_simples)):
        logger.info(f'Simples [{e+1}/{len(arquivos_simples)}]: {arquivos_simples[e]}')
        filepath = os.path.join(extracted_files, arquivos_simples[e])

        try:
            simples = pd.read_csv(filepath, sep=';', header=None,
                                   dtype=simples_dtypes, encoding='latin-1')
        except pd.errors.EmptyDataError:
            logger.info(f'Arquivo {arquivos_simples[e]} vazio. Pulando...')
            continue
        except Exception as ex:
            logger.error(f'Erro ao ler {arquivos_simples[e]}: {ex}')
            continue

        simples.columns = simples_columns
        await to_sql_async(simples, pool, 'simples')
        logger.info(f'Simples {arquivos_simples[e]} inserido ({len(simples):,} registros)')

        del simples
        gc.collect()

    elapsed = time.time() - simples_start
    console.print(f"[green]SIMPLES concluido em {elapsed:.1f}s[/green]")


async def process_outros_arquivos(pool):
    """Processa tabelas auxiliares (CNAE, Motivo, Municipio, etc.)"""
    console.print("\n[bold cyan]Processando tabelas auxiliares...[/bold cyan]")

    aux_dtypes = {0: 'Int32', 1: 'object'}

    for arquivo_tipo, nome_tabela in [
        (arquivos_cnae, 'cnae'),
        (arquivos_moti, 'motivo'),
        (arquivos_munic, 'municipio'),
        (arquivos_natju, 'natureza'),
        (arquivos_pais, 'pais'),
        (arquivos_quals, 'qualificacao')
    ]:
        if arquivo_tipo:
            for e in range(len(arquivo_tipo)):
                filepath = os.path.join(extracted_files, arquivo_tipo[e])
                try:
                    df = pd.read_csv(filepath, sep=';', header=None,
                                      dtype=aux_dtypes, encoding='latin-1')
                except pd.errors.EmptyDataError:
                    continue
                except Exception as ex:
                    logger.error(f'Erro ao ler {nome_tabela} {arquivo_tipo[e]}: {ex}')
                    continue

                df.columns = ['codigo', 'descricao']
                await to_sql_async(df, pool, nome_tabela)
                logger.info(f'{nome_tabela} inserido')
                del df
                gc.collect()


# ============================================================
# LOG DE EXECUCAO NO BANCO
# ============================================================

ETL_EXECUCAO_DDL = """
CREATE TABLE IF NOT EXISTS etl_execucao (
    id SERIAL PRIMARY KEY,
    data_inicio TIMESTAMP NOT NULL,
    data_fim TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'em_andamento',
    ano_mes_dados VARCHAR(7),
    total_arquivos INTEGER DEFAULT 0,
    arquivos_baixados INTEGER DEFAULT 0,
    arquivos_atualizados INTEGER DEFAULT 0,
    arquivos_falha INTEGER DEFAULT 0,
    tabelas_carregadas INTEGER DEFAULT 0,
    tempo_download_seg NUMERIC(10,1),
    tempo_extracao_seg NUMERIC(10,1),
    tempo_carga_seg NUMERIC(10,1),
    tempo_total_seg NUMERIC(10,1),
    erro_mensagem TEXT,
    CONSTRAINT chk_status CHECK (status IN ('em_andamento', 'sucesso', 'falha'))
)
"""


async def verificar_execucao_anterior(pool, ano_mes):
    """Verifica se ja existe uma execucao com sucesso para o mes selecionado.
    Retorna True se pode prosseguir, False se deve abortar."""
    async with pool.acquire() as conn:
        await conn.execute(ETL_EXECUCAO_DDL)
        row = await conn.fetchrow(
            "SELECT id, data_inicio, data_fim "
            "FROM etl_execucao "
            "WHERE ano_mes_dados = $1 AND status = 'sucesso' "
            "ORDER BY data_fim DESC LIMIT 1",
            ano_mes,
        )
    if not row:
        return True

    data_fim = row['data_fim'].strftime('%Y-%m-%d %H:%M:%S')
    console.print(
        f"\n[bold yellow]O ETL para o mes [cyan]{ano_mes}[/cyan] ja foi concluido "
        f"com sucesso em [cyan]{data_fim}[/cyan] (execucao id={row['id']}).[/bold yellow]"
    )

    if cli_args.force:
        console.print("[yellow]Flag --force ativa, prosseguindo mesmo assim.[/yellow]")
        return True

    if not DEBUG_MODE:
        console.print("[yellow]Modo automatico: abortando para evitar reprocessamento. Use --force para forcar.[/yellow]")
        return False

    resp = input("Deseja executar novamente? (s/N): ").strip().lower()
    return resp in ('s', 'sim', 'y', 'yes')


async def registrar_inicio_execucao(pool, ano_mes):
    """Registra inicio de uma execucao ETL e retorna o id."""
    data_inicio = datetime.datetime.now()

    async with pool.acquire() as conn:
        await conn.execute(ETL_EXECUCAO_DDL)
        row_id = await conn.fetchval(
            "INSERT INTO etl_execucao (data_inicio, ano_mes_dados) VALUES ($1, $2) RETURNING id",
            data_inicio,
            ano_mes
        )
    logger.info(f"Execucao ETL registrada com id={row_id}")
    return row_id


async def atualizar_execucao(pool, exec_id, **kwargs):
    """Atualiza campos da execucao ETL."""
    if not kwargs:
        return
    set_parts = []
    values = []
    for i, (key, val) in enumerate(kwargs.items(), start=1):
        set_parts.append(f"{key} = ${i}")
        values.append(val)
    values.append(exec_id)
    sql = f"UPDATE etl_execucao SET {', '.join(set_parts)} WHERE id = ${len(values)}"
    async with pool.acquire() as conn:
        await conn.execute(sql, *values)


async def finalizar_execucao(pool, exec_id, status, erro_mensagem=None, **metricas):
    """Finaliza a execucao com status, tempo e metricas."""
    await atualizar_execucao(
        pool, exec_id,
        data_fim=datetime.datetime.now(),
        status=status,
        erro_mensagem=erro_mensagem,
        **metricas
    )
    logger.info(f"Execucao ETL id={exec_id} finalizada com status={status}")


# ============================================================
# MAIN
# ============================================================

async def main():
    console.print("\n[bold magenta]" + "="*50 + "[/bold magenta]")
    console.print("[bold magenta]     ETL RECEITA FEDERAL (pandas + swap)     [/bold magenta]")
    console.print("[bold magenta]" + "="*50 + "[/bold magenta]\n")

    start_time = time.time()
    ano_mes_dados = f"{ano}-{mes_formatado}"
    exec_id = None
    download_metrics = {}
    download_time = 0.0
    extract_time = 0.0

    # Verificar se ha checkpoint de execucao anterior
    checkpoint = load_checkpoint()
    resuming = checkpoint is not None

    if resuming:
        stage = checkpoint['stage']
        console.print(f"[bold yellow]Checkpoint encontrado: stage='{stage}' "
                       f"(salvo em {checkpoint['timestamp']})[/bold yellow]")
        console.print("[bold yellow]Retomando execucao de onde parou...[/bold yellow]\n")
    else:
        stage = None

    # Definir quais fases ja foram concluidas
    STAGE_ORDER = [
        'staging_created', 'empresa_done', 'estabelecimento_done',
        'socios_done', 'simples_done', 'all_data_loaded', 'swap_done'
    ]

    def stage_completed(required_stage):
        """Retorna True se o checkpoint indica que esta fase ja foi concluida."""
        if not resuming or stage is None:
            return False
        try:
            return STAGE_ORDER.index(stage) >= STAGE_ORDER.index(required_stage)
        except ValueError:
            return False

    try:
        # Verificar se ja foi executado com sucesso para este mes (antes de qualquer download)
        await create_database_if_not_exists()
        check_pool = await create_db_pool()
        try:
            if not await verificar_execucao_anterior(check_pool, ano_mes_dados):
                return
            
            # Registrar inicio da execucao no banco
            exec_id = await registrar_inicio_execucao(check_pool, ano_mes_dados)
        finally:
            await check_pool.close()

        # Fase 0 + 1 + 2: Download e extracao (pular se swap ja foi feito)
        if not stage_completed('swap_done'):
            # Fase 0: Inicializacao
            initialize()

            # Fase 1: Download
            console.print("\n[bold yellow][FASE 1] Download dos arquivos...[/bold yellow]")
            download_start = time.time()
            download_metrics = await download_all_files()
            download_time = time.time() - download_start
            console.print(f"[green]Download concluido em {download_time:.1f}s[/green]")

            # Fase 2: Extracao
            console.print("\n[bold yellow][FASE 2] Extracao dos arquivos...[/bold yellow]")
            extract_start = time.time()
            await extract_all_files()
            extract_time = time.time() - extract_start
            console.print(f"[green]Extracao concluida em {extract_time:.1f}s[/green]")

            classificar_arquivos_extraidos()
        else:
            console.print("[green]Fases 1-4 ja concluidas (swap feito). Pulando para indices...[/green]")

        # Fase 3: Carga no banco (staging)
        db_start = time.time()

        pool = await create_db_pool()

        # Iniciar watchdog para detectar banco inacessivel
        # Verifica a cada 60s; declara falha apos 5 checks consecutivos sem resposta (~5 min)
        current_task = asyncio.current_task()
        watchdog_task = asyncio.create_task(db_watchdog(pool, current_task, interval=60))

        try:
            await atualizar_execucao(
                pool, exec_id,
                total_arquivos=download_metrics.get('total', 0),
                arquivos_baixados=download_metrics.get('baixados', 0),
                arquivos_atualizados=download_metrics.get('atualizados', 0),
                arquivos_falha=download_metrics.get('falhas', 0),
                tempo_download_seg=round(download_time, 1),
                tempo_extracao_seg=round(extract_time, 1),
            )

            if not stage_completed('swap_done'):
                console.print("\n[bold yellow][FASE 3] Carga no banco (staging)...[/bold yellow]")

                # Criar schema staging com tabelas tipadas
                if not stage_completed('staging_created'):
                    await setup_staging_schema(pool)
                    save_checkpoint('staging_created')

                # Processar todas as tabelas -> staging
                tabelas_ok = 0

                if not stage_completed('empresa_done'):
                    await process_empresa_files(pool)
                    save_checkpoint('empresa_done')
                else:
                    console.print("[green]Empresa ja processada, pulando...[/green]")
                tabelas_ok += 1

                if not stage_completed('estabelecimento_done'):
                    await process_estabelecimento_files(pool)
                    save_checkpoint('estabelecimento_done')
                else:
                    console.print("[green]Estabelecimento ja processado, pulando...[/green]")
                tabelas_ok += 1

                if not stage_completed('socios_done'):
                    await process_socios_files(pool)
                    save_checkpoint('socios_done')
                else:
                    console.print("[green]Socios ja processado, pulando...[/green]")
                tabelas_ok += 1

                if not stage_completed('simples_done'):
                    await process_simples_files(pool)
                    save_checkpoint('simples_done')
                else:
                    console.print("[green]Simples ja processado, pulando...[/green]")
                tabelas_ok += 1

                if not stage_completed('all_data_loaded'):
                    await process_outros_arquivos(pool)
                    save_checkpoint('all_data_loaded')
                else:
                    console.print("[green]Tabelas auxiliares ja processadas, pulando...[/green]")
                tabelas_ok += 6  # cnae, motivo, municipio, natureza, pais, qualificacao

                # Fase 4: Swap para producao (atomico por tabela)
                console.print("\n[bold yellow][FASE 4] Swap para producao...[/bold yellow]")
                await swap_to_production(pool)
                save_checkpoint('swap_done')
            else:
                tabelas_ok = 10

            # Verificar se watchdog detectou problema antes de continuar
            if watchdog_task.done():
                watchdog_task.result()  # propaga a excecao se houver

            # Fase 5: Criar indices (na producao, apos swap)
            # Os indices usam IF NOT EXISTS, entao e seguro reexecutar
            console.print("\n[bold yellow][FASE 5] Criacao de indices...[/bold yellow]")
            await create_indexes(pool)

            # Limpar schema staging residual
            async with pool.acquire() as conn:
                await conn.execute('DROP SCHEMA IF EXISTS staging CASCADE')

            clear_checkpoint()

            db_time = time.time() - db_start
            total_time = time.time() - start_time

            # Registrar sucesso no banco
            await finalizar_execucao(
                pool, exec_id, 'sucesso',
                tabelas_carregadas=tabelas_ok,
                tempo_carga_seg=round(db_time, 1),
                tempo_total_seg=round(total_time, 1),
            )

        except asyncio.CancelledError as e:
            # Watchdog cancelou a task porque o banco ficou inacessivel
            msg = str(e) if str(e) else "Operacao cancelada (banco inacessivel detectado pelo watchdog)"
            logger.error(msg)
            db_time = time.time() - db_start
            total_time = time.time() - start_time
            raise ConnectionError(msg) from e
        except Exception as e:
            db_time = time.time() - db_start
            total_time = time.time() - start_time
            if exec_id:
                try:
                    await finalizar_execucao(
                        pool, exec_id, 'falha',
                        erro_mensagem=str(e)[:500],
                        tempo_carga_seg=round(db_time, 1),
                        tempo_total_seg=round(total_time, 1),
                    )
                except Exception:
                    logger.warning("Nao foi possivel registrar falha no banco (banco inacessivel?)")
            raise
        finally:
            watchdog_task.cancel()
            try:
                await watchdog_task
            except (asyncio.CancelledError, Exception):
                pass
            await pool.close()

        minutes = int(total_time // 60)
        seconds = int(total_time % 60)

        console.print(f"\n[bold green]" + "="*60 + "[/bold green]")
        console.print(f"[bold green]    CONCLUIDO EM {minutes}m {seconds}s ({total_time:.1f}s)    [/bold green]")
        console.print(f"[bold green]" + "="*60 + "[/bold green]")

        table = Table(title="Resumo de Performance")
        table.add_column("Fase", style="cyan")
        table.add_column("Tempo", style="magenta")
        table.add_row("Download", f"{download_time:.1f}s")
        table.add_row("Extracao", f"{extract_time:.1f}s")
        table.add_row("Banco (carga + swap + indices)", f"{db_time:.1f}s")
        table.add_row("Total", f"{total_time:.1f}s", style="bold")
        console.print(table)

        # Resumo de arquivos
        if download_metrics:
            dl_table = Table(title="Resumo de Arquivos")
            dl_table.add_column("Metrica", style="cyan")
            dl_table.add_column("Quantidade", style="magenta")
            dl_table.add_row("Total de arquivos", str(download_metrics.get('total', 0)))
            dl_table.add_row("Baixados (novos)", str(download_metrics.get('baixados', 0)))
            dl_table.add_row("Ja atualizados", str(download_metrics.get('atualizados', 0)))
            dl_table.add_row("Falhas", str(download_metrics.get('falhas', 0)))
            console.print(dl_table)

        console.print("\n[bold blue]Dados disponiveis no banco![/bold blue]")

        # Limpeza final dos arquivos baixados e extraidos
        if not DEBUG_MODE:
            console.print("\n[bold yellow]Limpando arquivos temporarios...[/bold yellow]")
            removed_dl = clean_directory(output_files)
            removed_ex = clean_directory(extracted_files)
            logger.info(f"Limpeza final: {removed_dl} downloads e {removed_ex} extraidos removidos")
            console.print("[green]Arquivos temporarios removidos com sucesso.[/green]")

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        console.print(f"\n[bold red]ERRO NO PROCESSO ETL: {e}[/bold red]")
        raise


MAX_PIPELINE_RETRIES = int(os.getenv('ETL_MAX_RETRIES', '10'))

def run_with_retry():
    """Executa o pipeline ETL com retry automatico."""
    for attempt in range(MAX_PIPELINE_RETRIES):
        try:
            asyncio.run(main())
            return
        except KeyboardInterrupt:
            console.print("\n[bold red]Execucao cancelada pelo usuario.[/bold red]")
            sys.exit(130)
        except SystemExit:
            raise
        except Exception as e:
            wait_time = min(2 ** attempt * 30, 600)
            console.print(
                f"\n[bold red]Pipeline falhou (tentativa {attempt + 1}/{MAX_PIPELINE_RETRIES}): {e}[/bold red]"
            )
            if attempt == MAX_PIPELINE_RETRIES - 1:
                console.print("[bold red]Todas as tentativas esgotadas. Abortando.[/bold red]")
                sys.exit(1)
            console.print(f"[yellow]Aguardando {wait_time}s antes de re-tentar...[/yellow]")
            time.sleep(wait_time)


if __name__ == "__main__":
    run_with_retry()
