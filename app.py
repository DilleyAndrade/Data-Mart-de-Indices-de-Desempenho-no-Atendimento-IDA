import pandas as pd
import os
from sqlalchemy import create_engine, text, Column, Integer, String, Numeric, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

file_list = ['files/SCM2015.ods', 'files/SMP2015.ods', 'files/STFC2015.ods']

# Conexão com o PostgreSQL

user = 'postgres'
password = 'beAnalytic'
host = 'db'
port = '5432'
dbname = 'IDA'

connection_string = (
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
)

engine = create_engine(connection_string)
Base = declarative_base()

# Definindo modelos com ORM ---
class DimTempo(Base):
    __tablename__ = 'dim_tempo'
    id_tempo = Column(Integer, primary_key=True)
    mes_referencia = Column(Date, nullable=False, unique=True)
    ano = Column(Integer)
    mes = Column(Integer)
    nome_mes = Column(String(20))

class DimServico(Base):
    __tablename__ = 'dim_servico'
    id_servico = Column(Integer, primary_key=True)
    nome_servico = Column(String(50), nullable=False, unique=True)

class DimGrupoEconomico(Base):
    __tablename__ = 'dim_grupo_economico'
    id_grupo_economico = Column(Integer, primary_key=True)
    nome_grupo = Column(String(255), nullable=False, unique=True)

class FatoIda(Base):
    __tablename__ = 'fato_ida'
    id_fato = Column(Integer, primary_key=True)
    id_tempo = Column(Integer, ForeignKey('dim_tempo.id_tempo'))
    id_servico = Column(Integer, ForeignKey('dim_servico.id_servico'))
    id_grupo_economico = Column(Integer, ForeignKey('dim_grupo_economico.id_grupo_economico'))
    ida_valor = Column(Numeric)

    tempo = relationship("DimTempo")
    servico = relationship("DimServico")
    grupo_economico = relationship("DimGrupoEconomico")


# Comando SQL para criar a view de análise
sql_create_view = """
CREATE OR REPLACE VIEW analise_taxa_variacao_ida AS
WITH
dados_preparados AS (
    SELECT
        dt.mes_referencia,
        dge.nome_grupo,
        fi.ida_valor,
        LAG(fi.ida_valor, 1) OVER (PARTITION BY dge.nome_grupo ORDER BY dt.mes_referencia) AS ida_anterior
    FROM fato_ida fi
    JOIN dim_tempo dt ON fi.id_tempo = dt.id_tempo
    JOIN dim_grupo_economico dge ON fi.id_grupo_economico = dge.id_grupo_economico
),
taxas_calculadas AS (
    SELECT
        mes_referencia,
        nome_grupo,
        ida_valor,
        CASE
            WHEN ida_anterior > 0
            THEN ((ida_valor - ida_anterior) / ida_anterior) * 100
            ELSE 0
        END AS taxa_variacao_individual
    FROM dados_preparados
),
taxa_media AS (
    SELECT
        mes_referencia,
        AVG(taxa_variacao_individual) AS taxa_variacao_media
    FROM taxas_calculadas
    GROUP BY mes_referencia
)
SELECT
    to_char(tc.mes_referencia, 'YYYY-MM') as mes,
    ROUND(tm.taxa_variacao_media, 2) as "Taxa de Variação Média",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'ALGAR' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "ALGAR",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'CLARO' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "CLARO",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'EMBRATEL' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "EMBRATEL",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'GVT' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "GVT",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'OI' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "OI",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'SKY' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "SKY",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'TIM' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "TIM",
    ROUND(MAX(CASE WHEN tc.nome_grupo = 'VIVO' THEN (tc.taxa_variacao_individual - tm.taxa_variacao_media) ELSE NULL END), 2) AS "VIVO"
FROM taxas_calculadas tc
JOIN taxa_media tm ON tc.mes_referencia = tm.mes_referencia
GROUP BY tc.mes_referencia, tm.taxa_variacao_media
ORDER BY tc.mes_referencia;
"""

#Ler arquivos .ods, gera pré-processamento, desempivota os dados e adiciona em um único DataFrame.
def process_ods_files(file_paths):
    df_consolidado = pd.DataFrame()
    for path in file_paths:
        try:
            print(f"Processando arquivo: {path}")
            
            data = pd.read_excel(path, engine='odf')
            df = pd.DataFrame(data)

            df = df.rename(
                columns={
                    'HISTÓRICO DE RESULTADOS DO ÍNDICE DE DESEMPENHO NO ATENDIMENTO (IDA)': 'grupo_economico',
                    'Unnamed: 1': 'variavel',
                    'Unnamed: 2': '2025-01', 'Unnamed: 3': '2025-02', 'Unnamed: 4': '2025-03',
                    'Unnamed: 5': '2025-04', 'Unnamed: 6': '2025-05', 'Unnamed: 7': '2025-06',
                    'Unnamed: 8': '2025-07', 'Unnamed: 9': '2025-08', 'Unnamed: 10': '2025-09',
                    'Unnamed: 11': '2025-10', 'Unnamed: 12': '2025-11', 'Unnamed: 13': '2025-12',
                }
            )

            df.fillna(0, inplace=True)
            
            df = df[df['variavel'] == 'Indicador de Desempenho no Atendimento (IDA)']
            
            servico = os.path.basename(path).split('2015')[0]
            
            colunas_medidas = df.columns[2:]
            df_despivotado = pd.melt(
                df,
                id_vars=['grupo_economico', 'variavel'],
                value_vars=colunas_medidas,
                var_name='mes_referencia',
                value_name='ida_valor'
            )
            
            # Adiciona a coluna de serviço
            df_despivotado['servico'] = servico

            # Concatena com o DataFrame consolidado
            df_consolidado = pd.concat([df_consolidado, df_despivotado], ignore_index=True)
            
        except Exception as e:
            print(f"Erro ao processar o arquivo {path}: {e}")
            
    # Organização e limpeza final do DataFrame consolidado
    if not df_consolidado.empty:
        df_consolidado.dropna(subset=['ida_valor'], inplace=True)
        df_consolidado.drop(columns=['variavel'], inplace=True)
        df_consolidado.rename(columns={'mes_referencia': 'mes'}, inplace=True)
        df_consolidado['mes'] = pd.to_datetime(df_consolidado['mes'], format='%Y-%m')
        
    return df_consolidado

# Populando as tabelas no banco de dados
def populate_data_mart(df_consolidado, engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        df_tempo = df_consolidado[['mes']].drop_duplicates().copy()
        df_tempo.rename(columns={'mes': 'mes_referencia'}, inplace=True)
        df_tempo['ano'] = df_tempo['mes_referencia'].dt.year
        df_tempo['mes'] = df_tempo['mes_referencia'].dt.month
        df_tempo['nome_mes'] = df_tempo['mes_referencia'].dt.strftime('%B')
        df_tempo.to_sql('dim_tempo', engine, if_exists='append', index=False)
        print("Tabela dim_tempo populada.")

        df_servico = df_consolidado[['servico']].drop_duplicates().copy()
        df_servico.rename(columns={'servico': 'nome_servico'}, inplace=True)
        df_servico.to_sql('dim_servico', engine, if_exists='append', index=False)
        print("Tabela dim_servico populada.")

        df_grupo = df_consolidado[['grupo_economico']].drop_duplicates().copy()
        df_grupo.rename(columns={'grupo_economico': 'nome_grupo'}, inplace=True)
        df_grupo.to_sql('dim_grupo_economico', engine, if_exists='append', index=False)
        print("Tabela dim_grupo_economico populada.")

        df_dim_tempo = pd.read_sql_table('dim_tempo', engine, columns=['id_tempo', 'mes_referencia'])
        df_dim_servico = pd.read_sql_table('dim_servico', engine, columns=['id_servico', 'nome_servico'])
        df_dim_grupo = pd.read_sql_table('dim_grupo_economico', engine, columns=['id_grupo_economico', 'nome_grupo'])

        # Faz a junção com o DataFrame consolidado para obter as chaves estrangeiras
        df_fato = pd.merge(df_consolidado, df_dim_tempo, left_on='mes', right_on='mes_referencia', how='left')
        df_fato = pd.merge(df_fato, df_dim_servico, left_on='servico', right_on='nome_servico', how='left')
        df_fato = pd.merge(df_fato, df_dim_grupo, left_on='grupo_economico', right_on='nome_grupo', how='left')
        
        # Seleciona as colunas necessárias para a tabela fato
        df_fato = df_fato[['id_tempo', 'id_servico', 'id_grupo_economico', 'ida_valor']]
        df_fato.dropna(inplace=True) 

        # Carrega o DataFrame na tabela fato
        df_fato.to_sql('fato_ida', engine, if_exists='append', index=False)
        
        print("Tabela fato_ida populada com sucesso!")
        session.commit()

    except Exception as e:
        print(f"Erro ao popular o data mart: {e}")
        session.rollback()
        raise
    finally:
        session.close()

# Criando a view para análise
def create_analysis_view(engine):
    try:
        with engine.connect() as connection:
            connection.execute(text(sql_create_view))
            connection.commit()
        print("View 'analise_taxa_variacao_ida' criada/atualizada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar a view: {e}")
        raise

# Carregando os dados no banco
def run_full_pipeline():
    try:
        # Processamento dos arquivos ODS
        print("Iniciando processamento dos arquivos ODS...")
        df_consolidado = process_ods_files(file_list)
        if df_consolidado.empty:
            print("Nenhum dado foi processado. Encerrando.")
            return

        print("\nDados consolidados, transformados e prontos para o Data Mart:")
        print(df_consolidado.head())
        
        # Criação e população do Data Mart
        print("\nConectando ao banco de dados e criando tabelas...")
        Base.metadata.create_all(engine)
        print("Tabelas criadas com sucesso (ou já existentes).")
        
        populate_data_mart(df_consolidado, engine)

        # Criação da view de análise
        create_analysis_view(engine)

    except Exception as e:
        print(f"\nOcorreu um erro crítico na pipeline: {e}")
    finally:
        engine.dispose()
        print("\nConexão com o banco de dados encerrada.")


if __name__ == '__main__':
    run_full_pipeline()
