-- ============================================================================
-- SCRIPT DE CONFIGURAÇÃO DO BANCO DE DADOS
-- Receita Federal - Dados Públicos CNPJ
-- ============================================================================

-- Configurações de performance para PostgreSQL
-- Execute como superusuário (postgres) antes de restaurar dados grandes

-- ============================================================================
-- 1. CONFIGURAÇÕES DE MEMÓRIA
-- ============================================================================

-- Aumentar memória para operações de trabalho
SET work_mem = '1GB';

-- Aumentar memória para operações de maintenance (CREATE INDEX, VACUUM)
SET maintenance_work_mem = '2GB';

-- Aumentar shared_buffers (requer reinicialização do PostgreSQL)
-- Adicionar no postgresql.conf: shared_buffers = 4GB

-- ============================================================================
-- 2. CONFIGURAÇÕES DE WAL (Write-Ahead Logging)
-- ============================================================================

-- Otimizar para inserção em lote
SET synchronous_commit = off;
SET wal_buffers = '16MB';

-- Configurações para postgresql.conf:
-- max_wal_size = 4GB
-- min_wal_size = 1GB
-- checkpoint_completion_target = 0.9

-- ============================================================================
-- 3. CONFIGURAÇÕES DE CONEXÃO
-- ============================================================================

-- Aumentar limite de conexões se necessário
-- max_connections = 200 (no postgresql.conf)

-- ============================================================================
-- 4. EXTENSÕES ÚTEIS
-- ============================================================================

-- Extensão para estatísticas de consultas
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Extensão para funções de texto
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ============================================================================
-- 5. ÍNDICES OTIMIZADOS
-- ============================================================================

-- Índices principais para consultas por CNPJ
CREATE INDEX IF NOT EXISTS idx_empresa_cnpj_basico ON empresa(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj_basico ON estabelecimento(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj_basico ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_simples_cnpj_basico ON simples(cnpj_basico);

-- Índices para filtros comuns
CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimento(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimento(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON estabelecimento(uf);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnae_principal ON estabelecimento(cnae_fiscal_principal);

-- Índices para buscas por nome
CREATE INDEX IF NOT EXISTS idx_empresa_razao_social ON empresa USING gin(to_tsvector('portuguese', razao_social));
CREATE INDEX IF NOT EXISTS idx_estabelecimento_nome_fantasia ON estabelecimento USING gin(to_tsvector('portuguese', nome_fantasia));

-- Índices para sócios
CREATE INDEX IF NOT EXISTS idx_socios_nome ON socios USING gin(to_tsvector('portuguese', nome_socio));
CREATE INDEX IF NOT EXISTS idx_socios_cpf_cnpj ON socios(cnpj_cpf_socio);

-- ============================================================================
-- 6. VIEWS ÚTEIS
-- ============================================================================

-- View para empresa completa (dados básicos + estabelecimentos)
CREATE OR REPLACE VIEW v_empresa_completa AS
SELECT 
    e.cnpj_basico,
    e.razao_social,
    e.natureza_juridica,
    nj.descricao as natureza_juridica_descricao,
    e.capital_social,
    e.porte_empresa,
    CASE 
        WHEN e.porte_empresa = '01' THEN 'Microempresa'
        WHEN e.porte_empresa = '03' THEN 'Empresa de Pequeno Porte'
        WHEN e.porte_empresa = '05' THEN 'Demais'
        ELSE 'Não informado'
    END as porte_empresa_descricao,
    
    -- Dados do estabelecimento matriz
    est.cnpj_ordem,
    est.cnpj_dv,
    est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv as cnpj_completo,
    est.nome_fantasia,
    est.situacao_cadastral,
    CASE 
        WHEN est.situacao_cadastral = '01' THEN 'NULA'
        WHEN est.situacao_cadastral = '02' THEN 'ATIVA'
        WHEN est.situacao_cadastral = '03' THEN 'SUSPENSA'
        WHEN est.situacao_cadastral = '04' THEN 'INAPTA'
        WHEN est.situacao_cadastral = '08' THEN 'BAIXADA'
        ELSE 'SITUAÇÃO DESCONHECIDA'
    END as situacao_cadastral_descricao,
    
    est.data_situacao_cadastral,
    est.cnae_fiscal_principal,
    cnae.descricao as cnae_principal_descricao,
    est.data_inicio_atividade,
    
    -- Endereço
    CONCAT(
        COALESCE(est.tipo_logradouro, ''), ' ',
        COALESCE(est.logradouro, ''), ', ',
        COALESCE(est.numero, ''), ' ',
        COALESCE(est.bairro, ''), ' - ',
        COALESCE(mun.descricao, ''), '/',
        COALESCE(est.uf, ''), ' ',
        COALESCE(est.cep, '')
    ) as endereco_completo,
    
    -- Contato
    CASE 
        WHEN est.ddd_1 IS NOT NULL AND est.telefone_1 IS NOT NULL 
        THEN CONCAT('(', est.ddd_1, ') ', est.telefone_1)
        ELSE NULL
    END as telefone,
    
    est.correio_eletronico as email

FROM empresa e
LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico AND est.cnpj_ordem = '0001'
LEFT JOIN natureza nj ON e.natureza_juridica = nj.codigo
LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo;

-- View para estabelecimentos ativos
CREATE OR REPLACE VIEW v_estabelecimentos_ativos AS
SELECT 
    est.cnpj_basico,
    est.cnpj_ordem,
    est.cnpj_dv,
    est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv as cnpj_completo,
    CASE 
        WHEN est.cnpj_ordem = '0001' THEN 'MATRIZ'
        ELSE 'FILIAL'
    END as tipo_estabelecimento,
    
    e.razao_social,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.data_situacao_cadastral,
    est.cnae_fiscal_principal,
    cnae.descricao as cnae_principal_descricao,
    est.data_inicio_atividade,
    
    -- Endereço
    est.logradouro,
    est.numero,
    est.bairro,
    mun.descricao as municipio_descricao,
    est.uf,
    est.cep,
    
    -- Contato
    est.ddd_1,
    est.telefone_1,
    est.correio_eletronico as email

FROM estabelecimento est
LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo
WHERE est.situacao_cadastral = '02'; -- Apenas estabelecimentos ativos

-- ============================================================================
-- 7. FUNÇÕES ÚTEIS
-- ============================================================================

-- Função para formatar CNPJ
CREATE OR REPLACE FUNCTION format_cnpj(cnpj text) 
RETURNS text AS $$
BEGIN
    IF length(cnpj) = 14 THEN
        RETURN substring(cnpj from 1 for 2) || '.' ||
               substring(cnpj from 3 for 3) || '.' ||
               substring(cnpj from 6 for 3) || '/' ||
               substring(cnpj from 9 for 4) || '-' ||
               substring(cnpj from 13 for 2);
    ELSIF length(cnpj) = 8 THEN
        RETURN substring(cnpj from 1 for 2) || '.' ||
               substring(cnpj from 3 for 3) || '.' ||
               substring(cnpj from 6 for 3);
    ELSE
        RETURN cnpj;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Função para buscar empresa por CNPJ (básico ou completo)
CREATE OR REPLACE FUNCTION buscar_empresa(cnpj_input text)
RETURNS TABLE(
    cnpj_basico text,
    razao_social text,
    situacao_cadastral text,
    cnae_principal text,
    municipio text,
    uf text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.cnpj_basico,
        e.razao_social,
        est.situacao_cadastral,
        cnae.descricao as cnae_principal,
        mun.descricao as municipio,
        est.uf
    FROM empresa e
    LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico AND est.cnpj_ordem = '0001'
    LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
    LEFT JOIN municipio mun ON est.municipio = mun.codigo
    WHERE e.cnpj_basico = CASE 
        WHEN length(cnpj_input) = 14 THEN substring(cnpj_input from 1 for 8)
        WHEN length(cnpj_input) = 8 THEN cnpj_input
        ELSE cnpj_input
    END;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 8. LOG DE EXECUCOES DO ETL
-- ============================================================================

-- Tabela para rastrear cada execucao do ETL
CREATE TABLE IF NOT EXISTS etl_execucao (
    id SERIAL PRIMARY KEY,
    data_inicio TIMESTAMP NOT NULL DEFAULT current_timestamp,
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
);

-- View para consultar historico de execucoes
CREATE OR REPLACE VIEW v_etl_historico AS
SELECT
    id,
    data_inicio,
    data_fim,
    status,
    ano_mes_dados,
    total_arquivos,
    arquivos_baixados,
    arquivos_atualizados,
    arquivos_falha,
    tabelas_carregadas,
    tempo_download_seg || 's' as tempo_download,
    tempo_extracao_seg || 's' as tempo_extracao,
    tempo_carga_seg || 's' as tempo_carga,
    tempo_total_seg || 's' as tempo_total,
    erro_mensagem
FROM etl_execucao
ORDER BY data_inicio DESC;

-- ============================================================================
-- 9. TRIGGERS PARA AUDITORIA (OPCIONAL)
-- ============================================================================

-- Tabela de auditoria para mudanças
CREATE TABLE IF NOT EXISTS auditoria (
    id SERIAL PRIMARY KEY,
    tabela VARCHAR(50) NOT NULL,
    operacao VARCHAR(10) NOT NULL,
    usuario VARCHAR(50) DEFAULT current_user,
    data_hora TIMESTAMP DEFAULT current_timestamp,
    dados_antigos JSONB,
    dados_novos JSONB
);

-- Função de trigger para auditoria
CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO auditoria (tabela, operacao, dados_antigos)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD));
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO auditoria (tabela, operacao, dados_antigos, dados_novos)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(OLD), row_to_json(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO auditoria (tabela, operacao, dados_novos)
        VALUES (TG_TABLE_NAME, TG_OP, row_to_json(NEW));
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 10. ESTATÍSTICAS E ANÁLISES
-- ============================================================================

-- Atualizar estatísticas das tabelas
ANALYZE empresa;
ANALYZE estabelecimento;
ANALYZE socios;
ANALYZE simples;
ANALYZE cnae;
ANALYZE municipio;
ANALYZE natureza;
ANALYZE qualificacao;
ANALYZE motivo;
ANALYZE pais;

-- ============================================================================
-- 11. GRANTS E PERMISSÕES
-- ============================================================================

-- Criar usuário somente leitura
-- CREATE USER receita_readonly WITH PASSWORD 'senha_segura';
-- GRANT CONNECT ON DATABASE receita_federal TO receita_readonly;
-- GRANT USAGE ON SCHEMA public TO receita_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO receita_readonly;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO receita_readonly;

-- Criar usuário para aplicação
-- CREATE USER receita_app WITH PASSWORD 'senha_aplicacao';
-- GRANT CONNECT ON DATABASE receita_federal TO receita_app;
-- GRANT USAGE ON SCHEMA public TO receita_app;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO receita_app;

-- ============================================================================
-- FINALIZAÇÃO
-- ============================================================================

-- Mostrar resumo do banco
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Verificar índices criados
SELECT 
    indexname,
    tablename,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;