#!/usr/bin/env python3
"""
Script para gerar dump completo do banco de dados da Receita Federal
e recriar em outro ambiente com estrutura e dados

Funcionalidades:
- Gerar dump completo (estrutura + dados)
- Gerar apenas estrutura (modelo)
- Restaurar banco em outro ambiente
- Validar integridade dos dados
"""

import asyncio
import asyncpg
import os
import sys
import subprocess
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
from rich.table import Table
from rich.panel import Panel

# Carregar variáveis de ambiente
load_dotenv()

console = Console()

# Configurações do banco de origem
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'receita_cnpj'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

# Tabelas do sistema (em ordem de dependência)
TABLES_ORDER = [
    'cnae',
    'natureza',
    'qualificacao',
    'motivo',
    'municipio',
    'pais',
    'empresa',
    'estabelecimento',
    'socios',
    'simples'
]

def get_pg_dump_command(output_file, schema_only=False, data_only=False):
    """Gera comando pg_dump com as opções adequadas"""
    cmd = [
        'pg_dump',
        f"--host={DB_CONFIG['host']}",
        f"--port={DB_CONFIG['port']}",
        f"--username={DB_CONFIG['user']}",
        f"--dbname={DB_CONFIG['database']}",
        '--verbose',
        '--no-password',
        '--format=custom',
        '--compress=9',
        f"--file={output_file}"
    ]
    
    if schema_only:
        cmd.append('--schema-only')
    elif data_only:
        cmd.append('--data-only')
    
    return cmd

def get_pg_restore_command(dump_file, target_db_config):
    """Gera comando pg_restore"""
    cmd = [
        'pg_restore',
        f"--host={target_db_config['host']}",
        f"--port={target_db_config['port']}",
        f"--username={target_db_config['user']}",
        f"--dbname={target_db_config['database']}",
        '--verbose',
        '--no-password',
        '--clean',
        '--create',
        dump_file
    ]
    
    return cmd

async def get_database_info():
    """Obtém informações do banco de dados"""
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # Informações gerais
        db_size = await conn.fetchval("SELECT pg_size_pretty(pg_database_size(current_database()))")
        
        # Informações das tabelas
        tables_info = await conn.fetch("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """)
        
        # Contagem de registros por tabela
        tables_count = {}
        for table in TABLES_ORDER:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                tables_count[table] = count
            except:
                tables_count[table] = 0
        
        return {
            'database_size': db_size,
            'tables_info': tables_info,
            'tables_count': tables_count,
            'total_records': sum(tables_count.values())
        }
        
    finally:
        await conn.close()

async def generate_model_script():
    """Gera script SQL com apenas a estrutura (modelo)"""
    console.print("[bold blue]📐 Gerando modelo da estrutura do banco...[/bold blue]")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_file = f"receita_cnpj_model_{timestamp}.sql"
    
    # Gerar dump apenas da estrutura
    cmd = get_pg_dump_command(model_file, schema_only=True)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_CONFIG['password']
    
    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            console.print(f"[green]✅ Modelo gerado: {model_file}[/green]")
            
            # Gerar arquivo de documentação
            doc_file = f"receita_cnpj_model_doc_{timestamp}.md"
            await generate_documentation(doc_file)
            
            return model_file
        else:
            console.print(f"[red]❌ Erro ao gerar modelo: {result.stderr}[/red]")
            return None
            
    except Exception as e:
        console.print(f"[red]❌ Erro ao executar pg_dump: {e}[/red]")
        return None

async def generate_full_dump():
    """Gera dump completo (estrutura + dados)"""
    console.print("[bold blue]💾 Gerando dump completo do banco...[/bold blue]")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_file = f"receita_cnpj_full_{timestamp}.dump"
    
    # Gerar dump completo
    cmd = get_pg_dump_command(dump_file)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_CONFIG['password']
    
    try:
        console.print("[yellow]⏳ Gerando dump... (pode demorar alguns minutos)[/yellow]")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Verificar tamanho do arquivo
            file_size = os.path.getsize(dump_file)
            file_size_mb = file_size / (1024 * 1024)
            
            console.print(f"[green]✅ Dump completo gerado: {dump_file}[/green]")
            console.print(f"[cyan]📏 Tamanho do arquivo: {file_size_mb:.1f} MB[/cyan]")
            
            return dump_file
        else:
            console.print(f"[red]❌ Erro ao gerar dump: {result.stderr}[/red]")
            return None
            
    except Exception as e:
        console.print(f"[red]❌ Erro ao executar pg_dump: {e}[/red]")
        return None

async def generate_sql_dump():
    """Gera dump SQL puro sem usar pg_dump"""
    console.print("[bold blue]🔧 Gerando dump SQL puro...[/bold blue]")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_file = f"receita_cnpj_sql_{timestamp}.sql"
    
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            # Cabeçalho do arquivo
            f.write("-- Dump SQL da Receita Federal\n")
            f.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-- Banco: receita_cnpj\n\n")
            
            f.write("-- Configurações iniciais\n")
            f.write("SET statement_timeout = 0;\n")
            f.write("SET lock_timeout = 0;\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("SET standard_conforming_strings = on;\n")
            f.write("SET check_function_bodies = false;\n")
            f.write("SET client_min_messages = warning;\n")
            f.write("SET row_security = off;\n\n")
            
            # Obter estrutura das tabelas
            console.print("[yellow]📋 Obtendo estrutura das tabelas...[/yellow]")
            
            for table_name in TABLES_ORDER:
                console.print(f"[cyan]  → Processando tabela: {table_name}[/cyan]")
                
                # Obter definição da tabela
                table_def = await conn.fetch("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = $1 AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, table_name)
                
                if table_def:
                    f.write(f"-- Estrutura da tabela {table_name}\n")
                    f.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
                    f.write(f"CREATE TABLE {table_name} (\n")
                    
                    columns = []
                    for col in table_def:
                        col_def = f"    {col['column_name']} {col['data_type']}"
                        if col['is_nullable'] == 'NO':
                            col_def += " NOT NULL"
                        if col['column_default']:
                            col_def += f" DEFAULT {col['column_default']}"
                        columns.append(col_def)
                    
                    f.write(",\n".join(columns))
                    f.write("\n);\n\n")
                    
                    # Obter dados da tabela
                    console.print(f"[yellow]  → Exportando dados de {table_name}...[/yellow]")
                    
                    try:
                        # Contar registros
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                        
                        if count > 0:
                            f.write(f"-- Dados da tabela {table_name} ({count:,} registros)\n")
                            
                            # Exportar dados em lotes
                            batch_size = 1000
                            offset = 0
                            
                            while offset < count:
                                rows = await conn.fetch(f"""
                                    SELECT * FROM {table_name} 
                                    ORDER BY {await get_primary_key(conn, table_name)} 
                                    LIMIT {batch_size} OFFSET {offset}
                                """)
                                
                                if rows:
                                    # Gerar INSERTs
                                    columns_names = list(rows[0].keys())
                                    f.write(f"INSERT INTO {table_name} ({', '.join(columns_names)}) VALUES\n")
                                    
                                    values_list = []
                                    for row in rows:
                                        values = []
                                        for value in row.values():
                                            if value is None:
                                                values.append("NULL")
                                            elif isinstance(value, str):
                                                # Escapar strings
                                                escaped_value = value.replace("'", "''")
                                                values.append(f"'{escaped_value}'")
                                            elif isinstance(value, bool):
                                                values.append("TRUE" if value else "FALSE")
                                            else:
                                                values.append(str(value))
                                        
                                        values_list.append(f"({', '.join(values)})")
                                    
                                    f.write(",\n".join(values_list))
                                    f.write(";\n\n")
                                
                                offset += batch_size
                                
                                # Mostrar progresso
                                progress = min(100, (offset / count) * 100)
                                console.print(f"[green]    Progresso: {progress:.1f}%[/green]")
                        
                        else:
                            f.write(f"-- Tabela {table_name} está vazia\n\n")
                    
                    except Exception as e:
                        console.print(f"[red]    ❌ Erro ao exportar dados de {table_name}: {e}[/red]")
                        f.write(f"-- Erro ao exportar dados de {table_name}: {e}\n\n")
            
            # Adicionar índices e constraints
            f.write("-- Criação de índices\n")
            f.write("CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);\n")
            f.write("CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);\n")
            f.write("CREATE INDEX IF NOT EXISTS estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);\n")
            f.write("CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);\n")
            f.write("CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);\n")
            f.write("CREATE INDEX IF NOT EXISTS estabelecimento_situacao ON estabelecimento(situacao_cadastral);\n")
            f.write("CREATE INDEX IF NOT EXISTS estabelecimento_municipio ON estabelecimento(municipio);\n\n")
            
            f.write("-- Fim do dump SQL\n")
            f.write("-- Dump gerado com sucesso!\n")
        
        await conn.close()
        
        # Verificar tamanho do arquivo
        file_size = os.path.getsize(sql_file)
        file_size_mb = file_size / (1024 * 1024)
        
        console.print(f"[green]✅ Dump SQL gerado: {sql_file}[/green]")
        console.print(f"[cyan]📏 Tamanho do arquivo: {file_size_mb:.1f} MB[/cyan]")
        
        return sql_file
        
    except Exception as e:
        console.print(f"[red]❌ Erro ao gerar dump SQL: {e}[/red]")
        return None

async def get_primary_key(conn, table_name):
    """Obtém a chave primária de uma tabela"""
    try:
        pk_result = await conn.fetchval("""
            SELECT column_name 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = $1 
                AND tc.constraint_type = 'PRIMARY KEY'
            LIMIT 1
        """, table_name)
        
        return pk_result or "ctid"  # Usar ctid como fallback
    except:
        return "ctid"

async def generate_documentation(doc_file):
    """Gera documentação do modelo"""
    info = await get_database_info()
    
    doc_content = f"""# Receita Federal - Modelo do Banco de Dados

## Informações Gerais
- **Banco**: {DB_CONFIG['database']}
- **Tamanho Total**: {info['database_size']}
- **Total de Registros**: {info['total_records']:,}
- **Data de Geração**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Estrutura das Tabelas

### Ordem de Dependência
```
{' -> '.join(TABLES_ORDER)}
```

### Detalhes das Tabelas

"""
    
    for table in TABLES_ORDER:
        count = info['tables_count'].get(table, 0)
        
        # Encontrar informações de tamanho
        table_info = next((t for t in info['tables_info'] if t['tablename'] == table), None)
        size = table_info['size'] if table_info else 'N/A'
        
        doc_content += f"""#### {table.upper()}
- **Registros**: {count:,}
- **Tamanho**: {size}
- **Descrição**: {get_table_description(table)}

"""
    
    doc_content += """
## Comandos de Restauração

### Restaurar apenas estrutura:
```bash
pg_restore --host=localhost --port=5432 --username=postgres --dbname=target_db --schema-only receita_cnpj_model_YYYYMMDD_HHMMSS.sql
```

### Restaurar dados completos:
```bash
pg_restore --host=localhost --port=5432 --username=postgres --dbname=target_db --clean --create receita_cnpj_full_YYYYMMDD_HHMMSS.dump
```

## Configuração Recomendada

### Parâmetros PostgreSQL para melhor performance:
```sql
-- Aumentar memória para operações grandes
SET work_mem = '1GB';
SET maintenance_work_mem = '2GB';
SET shared_buffers = '4GB';

-- Otimizar para inserção em lote
SET synchronous_commit = off;
SET checkpoint_segments = 32;
SET wal_buffers = 16MB;
```

### Índices Recomendados:
```sql
-- Índices principais para consultas
CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);
CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);
CREATE INDEX IF NOT EXISTS estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);
CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);
CREATE INDEX IF NOT EXISTS estabelecimento_situacao ON estabelecimento(situacao_cadastral);
CREATE INDEX IF NOT EXISTS estabelecimento_municipio ON estabelecimento(municipio);
```

## Validação dos Dados

### Verificar integridade:
```sql
-- Verificar total de registros
SELECT 
    'empresa' as tabela, COUNT(*) as registros FROM empresa
UNION ALL
SELECT 
    'estabelecimento' as tabela, COUNT(*) as registros FROM estabelecimento
UNION ALL
SELECT 
    'socios' as tabela, COUNT(*) as registros FROM socios
UNION ALL
SELECT 
    'simples' as tabela, COUNT(*) as registros FROM simples;

-- Verificar CNPJs únicos
SELECT COUNT(DISTINCT cnpj_basico) as empresas_unicas FROM empresa;
SELECT COUNT(DISTINCT cnpj_basico || cnpj_ordem || cnpj_dv) as estabelecimentos_unicos FROM estabelecimento;
```
"""
    
    with open(doc_file, 'w', encoding='utf-8') as f:
        f.write(doc_content)
    
    console.print(f"[green]✅ Documentação gerada: {doc_file}[/green]")

def get_table_description(table):
    """Retorna descrição da tabela"""
    descriptions = {
        'empresa': 'Dados básicos das empresas (CNPJ básico, razão social, natureza jurídica)',
        'estabelecimento': 'Dados dos estabelecimentos (matriz e filiais) com endereços e CNAEs',
        'socios': 'Informações dos sócios e representantes legais',
        'simples': 'Dados do regime tributário Simples Nacional e MEI',
        'cnae': 'Tabela de referência com códigos e descrições das atividades econômicas',
        'natureza': 'Tabela de referência com naturezas jurídicas',
        'qualificacao': 'Tabela de referência com qualificações de sócios e responsáveis',
        'motivo': 'Tabela de referência com motivos de situação cadastral',
        'municipio': 'Tabela de referência com códigos e nomes dos municípios',
        'pais': 'Tabela de referência com códigos e nomes dos países'
    }
    return descriptions.get(table, 'Tabela de dados da Receita Federal')

async def restore_database(dump_file, target_config):
    """Restaura banco de dados em outro ambiente"""
    console.print(f"[bold blue]🔄 Restaurando banco de dados de {dump_file}...[/bold blue]")
    
    cmd = get_pg_restore_command(dump_file, target_config)
    
    env = os.environ.copy()
    env['PGPASSWORD'] = target_config['password']
    
    try:
        console.print("[yellow]⏳ Restaurando... (pode demorar alguns minutos)[/yellow]")
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            console.print("[green]✅ Banco restaurado com sucesso![/green]")
            return True
        else:
            console.print(f"[red]❌ Erro ao restaurar: {result.stderr}[/red]")
            return False
            
    except Exception as e:
        console.print(f"[red]❌ Erro ao executar pg_restore: {e}[/red]")
        return False

async def validate_restored_database(target_config):
    """Valida integridade do banco restaurado"""
    console.print("[bold blue]🔍 Validando integridade do banco restaurado...[/bold blue]")
    
    try:
        conn = await asyncpg.connect(**target_config)
        
        # Verificar se todas as tabelas existem
        existing_tables = await conn.fetch("""
            SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        """)
        
        existing_table_names = {row['tablename'] for row in existing_tables}
        
        console.print(f"[cyan]📊 Tabelas encontradas: {len(existing_table_names)}[/cyan]")
        
        # Verificar contagem de registros
        table = Table()
        table.add_column("Tabela", style="cyan")
        table.add_column("Registros", style="yellow")
        table.add_column("Status", style="green")
        
        total_records = 0
        
        for table_name in TABLES_ORDER:
            if table_name in existing_table_names:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    total_records += count
                    table.add_row(table_name, f"{count:,}", "✅ OK")
                except Exception as e:
                    table.add_row(table_name, "0", f"❌ Erro: {e}")
            else:
                table.add_row(table_name, "0", "❌ Não encontrada")
        
        console.print(table)
        console.print(f"[bold green]📈 Total de registros: {total_records:,}[/bold green]")
        
        await conn.close()
        
        return total_records > 0
        
    except Exception as e:
        console.print(f"[red]❌ Erro na validação: {e}[/red]")
        return False

def get_target_config():
    """Obtém configuração do banco de destino"""
    console.print("[bold yellow]🎯 Configuração do banco de destino:[/bold yellow]")
    
    target_config = {}
    target_config['host'] = input("Host [localhost]: ") or 'localhost'
    target_config['port'] = int(input("Port [5432]: ") or '5432')
    target_config['database'] = input("Database [receita_cnpj_restored]: ") or 'receita_cnpj_restored'
    target_config['user'] = input("User [postgres]: ") or 'postgres'
    target_config['password'] = input("Password: ")
    
    return target_config

async def main():
    """Função principal"""
    console.print("\n[bold magenta]" + "="*60 + "[/bold magenta]")
    console.print("[bold magenta]    DUMP E RESTAURAÇÃO - RECEITA FEDERAL    [/bold magenta]")
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]\n")
    
    if len(sys.argv) < 2:
        console.print("[red]Uso: python dump_and_restore.py <comando>[/red]")
        console.print("\n[yellow]Comandos disponíveis:[/yellow]")
        console.print("  [cyan]info[/cyan]     - Exibir informações do banco")
        console.print("  [cyan]model[/cyan]    - Gerar apenas modelo (estrutura)")
        console.print("  [cyan]dump[/cyan]     - Gerar dump completo (estrutura + dados)")
        console.print("  [cyan]sql[/cyan]      - Gerar dump SQL puro (sem pg_dump)")
        console.print("  [cyan]restore[/cyan]  - Restaurar banco em outro ambiente")
        console.print("  [cyan]all[/cyan]      - Gerar modelo + dump completo")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'info':
            # Exibir informações do banco
            info = await get_database_info()
            
            console.print(f"[bold blue]📊 Informações do Banco de Dados[/bold blue]")
            console.print(f"[cyan]Tamanho Total: {info['database_size']}[/cyan]")
            console.print(f"[cyan]Total de Registros: {info['total_records']:,}[/cyan]")
            
            # Tabela com informações das tabelas
            table_info = Table()
            table_info.add_column("Tabela", style="cyan")
            table_info.add_column("Registros", style="yellow")
            table_info.add_column("Tamanho", style="green")
            
            for table_name in TABLES_ORDER:
                count = info['tables_count'].get(table_name, 0)
                table_data = next((t for t in info['tables_info'] if t['tablename'] == table_name), None)
                size = table_data['size'] if table_data else 'N/A'
                
                table_info.add_row(table_name, f"{count:,}", size)
            
            console.print(table_info)
            
        elif command == 'model':
            # Gerar apenas modelo
            model_file = await generate_model_script()
            if model_file:
                console.print(f"[green]🎉 Modelo gerado com sucesso: {model_file}[/green]")
                
        elif command == 'dump':
            # Gerar dump completo
            dump_file = await generate_full_dump()
            if dump_file:
                console.print(f"[green]🎉 Dump completo gerado: {dump_file}[/green]")
                
        elif command == 'sql':
            # Gerar dump SQL puro
            sql_file = await generate_sql_dump()
            if sql_file:
                console.print(f"[green]🎉 Dump SQL gerado: {sql_file}[/green]")
                
        elif command == 'restore':
            # Restaurar banco
            if len(sys.argv) < 3:
                console.print("[red]Uso: python dump_and_restore.py restore <dump_file>[/red]")
                sys.exit(1)
            
            dump_file = sys.argv[2]
            if not os.path.exists(dump_file):
                console.print(f"[red]❌ Arquivo não encontrado: {dump_file}[/red]")
                sys.exit(1)
            
            target_config = get_target_config()
            
            if await restore_database(dump_file, target_config):
                if await validate_restored_database(target_config):
                    console.print("[green]🎉 Banco restaurado e validado com sucesso![/green]")
                else:
                    console.print("[yellow]⚠️  Banco restaurado mas com problemas na validação[/yellow]")
            
        elif command == 'all':
            # Gerar modelo + dump completo
            console.print("[bold blue]🔄 Gerando modelo e dump completo...[/bold blue]")
            
            model_file = await generate_model_script()
            dump_file = await generate_full_dump()
            
            if model_file and dump_file:
                console.print("[green]🎉 Modelo e dump gerados com sucesso![/green]")
                console.print(f"[cyan]📐 Modelo: {model_file}[/cyan]")
                console.print(f"[cyan]💾 Dump: {dump_file}[/cyan]")
            
        else:
            console.print(f"[red]❌ Comando inválido: {command}[/red]")
            sys.exit(1)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Processo interrompido pelo usuário[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]❌ Erro no processo: {e}[/red]")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())