# Projeto ONS Machine Learning - dbt + Snowflake

## Visão Geral

Projeto de transformação de dados utilizando dbt Core para análise e modelagem de dados do setor energético brasileiro (ONS - Operador Nacional do Sistema Elétrico).

## Arquitetura

- **Data Warehouse**: Snowflake (LAB_ONS)
- **Transformação**: dbt Core 1.10.11
- **Adapter**: dbt-snowflake 1.10.2
- **Ambiente**: Python Virtual Environment (venv)
- **Versionamento**: Git/GitHub

## Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)
- Acesso ao Snowflake com credenciais válidas
- Git instalado

## Instalação

### 1. Clone o repositório

git clone https://github.com/israelmteixeira1/pos-ia-mod-2.git
cd pos-ia-mod-2

### 2. Crie e ative o ambiente virtual

python3 -m venv .venv
source .venv/bin/activate

### 3. Instale as dependências do dbt

pip install dbt-core==1.10.11 dbt-snowflake==1.10.2


### Passo 4: Verifique a instalação do dbt

dbt --version


### Passo 5: Configure o profiles.yml

#### 5.1. Crie o diretório .dbt

mkdir -p ~/.dbt

#### 5.2. Crie o arquivo profiles.yml

**Linux/macOS:**

nano ~/.dbt/profiles.yml

#### 5.3. Cole essas informações no arquivo profiles.yml

ons_ml:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: zmjnstx-ey20159
      user: awsposifif
      password: 'Aws@@ifg202522'
      role: ACCOUNTADMIN
      database: LAB_ONS
      warehouse: LAB_WH
      schema: STAGING_ONS
      threads: 4
      
#### 5.4. Salve o arquivo

**No nano (Linux/macOS):**
- Pressione `Ctrl + X`
- Pressione `Y` para confirmar
- Pressione `Enter`

**No Notepad (Windows):**
- Vá em Arquivo → Salvar
- Feche o Notepad

#### 5.5. Verifique se o arquivo foi criado corretamente

cat ~/.dbt/profiles.yml

### Passo 6: Teste a conexão com Snowflake

dbt debug

**Resultado esperado** (no final da saída):

Connection test: [OK connection ok]

All checks passed!


## Estrutura do Projeto

## Estrutura do Projeto

**Diretórios:**
- `.venv/` - Ambiente virtual Python (não versionado)
- `dags/` - DAGs do Airflow para orquestração
- `dbt_packages/` - Pacotes dbt instalados (não versionado)
- `macros/` - Macros SQL reutilizáveis
- `models/` - Modelos dbt
  - `staging/` - Camada de staging (dados brutos normalizados)
  - `core/` - Modelos finais (fatos e dimensões)

**Arquivos:**
- `.gitignore` - Arquivos ignorados pelo Git
- `dbt_project.yml` - Configuração principal do projeto
- `package-lock.yml` - Lock de versões de pacotes
- `packages.yml` - Dependências de pacotes dbt
- `README.md` - Este arquivo



## Comandos Principais do dbt

### Instalação e Configuração

Instalar/atualizar pacotes dbt
dbt deps

Verificar conexão e configuração
dbt debug

Executar todos os modelos
dbt run

Executar modelo específico
dbt run --select nome_do_modelo

Executar modelos de uma camada
dbt run --select staging.*

Limpar arquivos compilados
dbt clean

Executar todos os testes
dbt test
