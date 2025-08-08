# 📊 Data Mart de Índices de Desempenho no Atendimento (IDA)

Este repositório automatiza a criação de um **Data Mart** no PostgreSQL com dados da Anatel sobre o **Índice de Desempenho no Atendimento (IDA)**.  
O processo inclui:

- Extração de arquivos `.ods`
- Transformação e normalização dos dados
- Carga no banco de dados com SQLAlchemy (ORM)
- Criação de uma view analítica para cálculo da **taxa de variação média** e comparação entre grupos econômicos
- Entrega via **Docker Compose** com PostgreSQL, PgAdmin e aplicação Python

---

## 📂 Estrutura do Projeto

```
├── app.py                # Script principal da pipeline ETL + criação da view
├── docker-compose.yaml   # Configuração dos serviços (PostgreSQL, PgAdmin e aplicação)
├── Dockerfile            # Configuração da imagem da aplicação Python
├── files/                # Pasta com arquivos .ods de entrada
│   ├── SCM2015.ods
│   ├── SMP2015.ods
│   └── STFC2015.ods
└── README.md             # Este arquivo
```

---

## 🛠️ Pré-requisitos

Antes de rodar o projeto, você precisa ter instalado:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- Arquivos `.ods` da Anatel na pasta `files/`

---

## 🚀 Como Rodar o Projeto

### 1️⃣ Clonar o repositório
```bash
git clone https://github.com/seu-usuario/seu-repo.git
cd seu-repo
```

### 2️⃣ Adicionar os arquivos de dados
Coloque os arquivos `.ods` na pasta `files/`:
```
files/
├── SCM2015.ods
├── SMP2015.ods
└── STFC2015.ods
```

### 3️⃣ Subir os containers
```bash
docker-compose up --build
```

Isso irá:
- Criar o banco PostgreSQL (`IDA`)
- Subir o **PgAdmin** para visualização e consulta
- Rodar o script Python (`app.py`) que processa os arquivos e popula o Data Mart

---

## 📋 Funcionalidades do Script (`app.py`)

### **1. Processamento dos arquivos ODS**
Função: `process_ods_files(file_list)`
- Lê todos os arquivos `.ods` listados
- Renomeia colunas
- Filtra apenas os dados do indicador **IDA**
- Desempivota os dados (transforma colunas de meses em linhas)
- Adiciona coluna com o tipo de serviço (SCM, SMP, STFC)

### **2. Criação das tabelas**
As tabelas são criadas via SQLAlchemy ORM:
- **dim_tempo**
- **dim_servico**
- **dim_grupo_economico**
- **fato_ida**

### **3. Carga de dados no banco**
Função: `populate_data_mart(df_consolidado, engine)`
- Popula tabelas dimensão
- Associa chaves estrangeiras
- Popula a tabela fato

### **4. Criação da view de análise**
Função: `create_analysis_view(engine)`
- Cria a view `analise_taxa_variacao_ida` com:
  - Taxa de variação média
  - Diferença de cada grupo econômico em relação à média

---

## 📊 Estrutura do Banco de Dados

**Tabelas:**
- `dim_tempo`  
  - id_tempo (PK)  
  - mes_referencia  
  - ano  
  - mes  
  - nome_mes  

- `dim_servico`  
  - id_servico (PK)  
  - nome_servico  

- `dim_grupo_economico`  
  - id_grupo_economico (PK)  
  - nome_grupo  

- `fato_ida`  
  - id_fato (PK)  
  - id_tempo (FK)  
  - id_servico (FK)  
  - id_grupo_economico (FK)  
  - ida_valor  

**View:**
- `analise_taxa_variacao_ida`  
  - Mês (`YYYY-MM`)
  - Taxa de Variação Média
  - Diferença de cada grupo em relação à média

---

## 🔍 Consultando os Dados no PgAdmin

Após subir o projeto:

1. Acesse o **PgAdmin** em [http://localhost:8080](http://localhost:8080)
2. Login:
   - **Email:** `admin@docker.com`
   - **Senha:** `beAnalytic`
3. Conecte-se ao servidor PostgreSQL com:
   - **Host:** `db`
   - **Port:** `5432`
   - **User:** `postgres`
   - **Password:** `beAnalytic`
   - **Database:** `IDA`
4. Execute consultas SQL, por exemplo:
```sql
SELECT * FROM analise_taxa_variacao_ida;
```

---

## 📦 Parar e Remover Containers
```bash
docker-compose down
```

---

## 🧩 Possíveis Erros

- **Erro de conexão com o banco** → Verifique se o serviço `db` do Docker está ativo.
- **Arquivo .ods não encontrado** → Confirme que está na pasta `files/` com o nome correto.
- **View não criada** → Verifique se as tabelas fato e dimensões foram populadas antes.

---

## 👤 Autor

**Dilley Andrade**  
Engenheiro de Dados | SQL | ETL | Python — Focado em soluções de dados, ETL, BI e engenharia de dados. (81) 98663-2609 | dilleyandrade@gmail.com | http://linkedin.com/in/dilleyandrade | http://github.com/DilleyAndrade

---

## 📜 Licença
Este projeto está sob a licença MIT. Sinta-se livre para usar e modificar.
# Data-Mart-de-ndices-de-Desempenho-no-Atendimento-IDA-
