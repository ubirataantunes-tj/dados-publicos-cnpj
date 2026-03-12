#!/usr/bin/env python3
"""
Gerador de dump SQL usando apenas comandos SQL
(alternativa quando pg_dump não está disponível)
"""

import asyncio
import asyncpg
import os
from datetime import datetime
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()
console = Console()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'receita_cnpj'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
}

async def generate_sql_structure():
    """Gera estrutura do banco usando comandos SQL"""
    conn = await asyncpg.connect(**DB_CONFIG)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_file = f"receita_cnpj_model_sql_{timestamp}.sql"
    
    try:
        console.print("[bold blue]📐 Gerando modelo SQL da estrutura...[/bold blue]")
        
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Modelo do Banco de Dados - Receita Federal CNPJ\n")
            f.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- Banco: {DB_CONFIG['database']}\n\n")
            
            # Obter lista de tabelas
            tables = await conn.fetch("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            """)
            
            for table in tables:
                table_name = table['tablename']
                console.print(f"[cyan]Processando tabela: {table_name}[/cyan]")
                
                # Obter estrutura da tabela
                columns = await conn.fetch(f"""
                    SELECT 
                        column_name,
                        data_type,
                        character_maximum_length,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                
                f.write(f"-- Tabela: {table_name}\n")
                f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (\n")
                
                column_defs = []
                for col in columns:
                    col_def = f"    {col['column_name']} {col['data_type']}"
                    
                    if col['character_maximum_length']:
                        col_def += f"({col['character_maximum_length']})"
                    
                    if col['is_nullable'] == 'NO':
                        col_def += " NOT NULL"
                    
                    if col['column_default']:
                        col_def += f" DEFAULT {col['column_default']}"
                    
                    column_defs.append(col_def)
                
                f.write(",\n".join(column_defs))
                f.write("\n);\n\n")
                
                # Adicionar índices
                indexes = await conn.fetch(f"""
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE tablename = '{table_name}'
                    AND schemaname = 'public'
                    AND indexname NOT LIKE '%_pkey'
                """)
                
                for idx in indexes:
                    f.write(f"-- Índice: {idx['indexname']}\n")
                    f.write(f"{idx['indexdef']};\n\n")
        
        console.print(f"[green]✅ Modelo SQL gerado: {model_file}[/green]")
        return model_file
        
    finally:
        await conn.close()

async def generate_sample_data():
    """Gera amostra de dados para testes"""
    conn = await asyncpg.connect(**DB_CONFIG)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sample_file = f"receita_cnpj_sample_{timestamp}.sql"
    
    try:
        console.print("[bold blue]📊 Gerando amostra de dados...[/bold blue]")
        
        with open(sample_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Amostra de Dados - Receita Federal CNPJ\n")
            f.write(f"-- Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Tabelas de referência (dados completos)
            ref_tables = ['cnae', 'natureza', 'qualificacao', 'motivo', 'municipio', 'pais']
            
            for table in ref_tables:
                console.print(f"[cyan]Exportando dados completos: {table}[/cyan]")
                
                # Obter colunas
                columns = await conn.fetch(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                
                col_names = [col['column_name'] for col in columns]
                col_list = ", ".join(col_names)
                
                # Obter dados
                rows = await conn.fetch(f"SELECT {col_list} FROM {table} ORDER BY {col_names[0]} LIMIT 1000")
                
                if rows:
                    f.write(f"-- Dados da tabela: {table}\n")
                    f.write(f"INSERT INTO {table} ({col_list}) VALUES\n")
                    
                    value_strings = []
                    for row in rows:
                        values = []
                        for val in row:
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, str):
                                values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                            else:
                                values.append(str(val))
                        value_strings.append(f"({', '.join(values)})")
                    
                    f.write(",\n".join(value_strings))
                    f.write(";\n\n")
            
            # Tabelas principais (amostras)
            main_tables = ['empresa', 'estabelecimento', 'socios', 'simples']
            
            for table in main_tables:
                console.print(f"[cyan]Exportando amostra: {table}[/cyan]")
                
                # Obter colunas
                columns = await conn.fetch(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                
                col_names = [col['column_name'] for col in columns]
                col_list = ", ".join(col_names)
                
                # Obter amostra de dados
                rows = await conn.fetch(f"SELECT {col_list} FROM {table} ORDER BY {col_names[0]} LIMIT 100")
                
                if rows:
                    f.write(f"-- Amostra da tabela: {table} (100 registros)\n")
                    f.write(f"INSERT INTO {table} ({col_list}) VALUES\n")
                    
                    value_strings = []
                    for row in rows:
                        values = []
                        for val in row:
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, str):
                                values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
                            else:
                                values.append(str(val))
                        value_strings.append(f"({', '.join(values)})")
                    
                    f.write(",\n".join(value_strings))
                    f.write(";\n\n")
        
        console.print(f"[green]✅ Amostra de dados gerada: {sample_file}[/green]")
        return sample_file
        
    finally:
        await conn.close()

async def main():
    """Função principal"""
    console.print("\n[bold magenta]🗄️ Gerador de Dump SQL - Receita Federal[/bold magenta]\n")
    
    try:
        # Gerar estrutura
        model_file = await generate_sql_structure()
        
        # Gerar amostra de dados
        sample_file = await generate_sample_data()
        
        console.print(f"\n[bold green]🎉 Arquivos gerados com sucesso![/bold green]")
        console.print(f"[cyan]📐 Estrutura: {model_file}[/cyan]")
        console.print(f"[cyan]📊 Amostra: {sample_file}[/cyan]")
        
        # Gerar script de instalação
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        install_script = f"""#!/bin/bash
# Script de instalação do banco Receita Federal
# Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

DB_HOST="${{DB_HOST:-{db_host}}}"
DB_PORT="${{DB_PORT:-{db_port}}}"
DB_USER="${{DB_USER:-{db_user}}}"

echo "Criando banco de dados..."
createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" receita_cnpj_restored

echo "Aplicando estrutura..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d receita_cnpj_restored -f {model_file}

echo "Inserindo dados de amostra..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d receita_cnpj_restored -f {sample_file}

echo "Aplicando configurações..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d receita_cnpj_restored -f src/database_setup.sql

echo "Banco instalado com sucesso!"
"""
        
        with open('install_database.sh', 'w') as f:
            f.write(install_script)
        
        os.chmod('install_database.sh', 0o755)
        console.print(f"[green]✅ Script de instalação: install_database.sh[/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Erro: {e}[/red]")

if __name__ == "__main__":
    asyncio.run(main())