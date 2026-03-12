# 🤖 CLAUDE.md - Documentação de Melhorias do Sistema ETL

## 📋 Resumo das Melhorias Implementadas

Este documento detalha as melhorias implementadas no sistema ETL de dados públicos do CNPJ para resolver problemas de conectividade, configuração e robustez do processo.

## 🛠️ Melhorias Técnicas Implementadas

### 1. **Sistema de Seleção Dinâmica de Data** 🗓️
- **Funcionalidade**: Interface interativa para seleção de ano/mês dos dados
- **Implementação**: Função `get_year_month()` com validação robusta
- **Benefícios**:
  - Usuário pode escolher qualquer mês/ano disponível (2019 - atual)
  - Validação automática de entrada
  - URL construída dinamicamente: `{ano}-{mes:02d}`
  - Valores padrão baseados na data atual

### 2. **Tratamento Robusto de Conexões SSL/TLS** 🔐
- **Problema Original**: Falhas de conexão com certificados SSL da Receita Federal
- **Soluções Implementadas**:
  - Configuração SSL permissiva (`ssl_context.verify_mode = ssl.CERT_NONE`)
  - Retry automático com backoff exponencial
  - User-Agent atualizado para simular navegador moderno
  - Timeouts apropriados (30s requisições, 120s downloads)
  - Tratamento específico de `ConnectError`, `TimeoutException` e `SSLError`

### 3. **Sistema de Recuperação de Falhas** ⚡
- **Função `get_html_with_retry()`**: Até 3 tentativas para acessar página principal
- **Downloads assíncronos robustos**: Configuração SSL e limites de conexão otimizados
- **Logs detalhados**: Identificação precisa de pontos de falha

### 4. **Correção de Configurações do Ambiente** ⚙️
- **Variáveis adicionadas ao .env**:
  - `OUTPUT_FILES_PATH=./dados/downloads`
  - `EXTRACTED_FILES_PATH=./dados/extracted`
- **Problema resolvido**: Erro de diretórios `NoneType` durante extração

### 5. **Melhorias na Função `check_diff()`** 📁
- Aplicação das mesmas configurações SSL robustas
- Tratamento de exceções para verificação de arquivos
- Fallback seguro em caso de erro (força download)

## 🚀 Comandos de Teste e Validação

### Testar Conectividade com Banco
```bash
nc -zv localhost 5436
```

### Verificar Status dos Containers
```bash
docker ps | grep postgres
```

### Executar ETL
```bash
uv run src/etl/ETL_dados_publicos_empresas.py
```

## 📊 Impacto das Melhorias

### Antes das Melhorias:
- ❌ Falhas SSL frequentes
- ❌ Downloads interrompidos (0/37 arquivos)
- ❌ Extrações falhando por variáveis `None`
- ❌ Erro de conexão com banco (`Connection reset by peer`)

### Depois das Melhorias:
- ✅ Conexões SSL robustas com retry automático
- ✅ Downloads funcionando com configuração otimizada
- ✅ Extrações funcionais com caminhos corretos
- ✅ Seleção interativa de ano/mês
- ✅ Tratamento gracioso de erros com mensagens informativas

## 🔧 Configuração Recomendada

### Arquivo .env (variáveis obrigatórias):
```bash
# BANCO DE DADOS
DB_HOST=localhost
DB_PORT=5436
DB_NAME=receita_cnpj
DB_USER=postgres
DB_PASSWORD=sua_senha

# CAMINHOS OBRIGATÓRIOS
OUTPUT_FILES_PATH=./dados/downloads
EXTRACTED_FILES_PATH=./dados/extracted
```

## 🐛 Troubleshooting

### Se ainda encontrar problemas SSL:
1. Verifique conectividade: `curl -I https://arquivos.receitafederal.gov.br`
2. Confirme data/hora do sistema
3. Execute com logs detalhados

### Se downloads falharem:
1. Verifique espaço em disco
2. Confirme permissões nos diretórios de destino
3. Teste conectividade de rede

### Se banco não conectar:
1. Confirme que PostgreSQL está rodando: `docker ps | grep postgres`
2. Teste conectividade: `nc -zv localhost 5436`
3. Verifique credenciais no .env

## 📝 Notas de Desenvolvimento

- **Python 3.13**: Compatibilidade total testada
- **Bibliotecas principais**: `httpx`, `asyncpg`, `pandas`, `rich`
- **Padrão de SSL**: Permissivo para contornar limitações dos servidores governamentais
- **Logs**: Rich console com formatação colorida e progress bars

---

**🤖 Gerado com Claude Code**  
Data: 2025-08-15  
Versão: ETL v2.1 com melhorias SSL e interface interativa