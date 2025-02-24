from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyodbc
import os
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from datetime import datetime

# ========================================== OBS: Questão 2 - C infelizmente não tenho conhecimento suficiente para responser
load_dotenv()
driver = os.getenv('DRIVER_NAME')
server_name = os.getenv('SERVER_NAME')
database_name = os.getenv('DATABASE_NAME')
trusted_connection = os.getenv('TRUSTED_CONNECTION')

spark = SparkSession.builder.appName('ETL_Anuario').getOrCreate()

# Configuração de conexão com o SQL Server
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server_name};DATABASE={database_name};TRUSTED_CONNECTION={trusted_connection}')
cursor = conn.cursor()

# Função para carregar o arquivo TXT e transformar os dados
def carregar_arquivo_txt(arquivo: str) -> DataFrame:
    """
    Carrega um arquivo TXT delimitado por ponto e vírgula (;) e transforma os dados em um DataFrame do Spark.
    
    :param arquivo (str): Caminho do arquivo TXT a ser carregado.
    
    :return DataFrame: DataFrame contendo os dados do arquivo, com as colunas inferidas.
    
    A função usa a biblioteca PySpark para ler o arquivo e transformar em um DataFrame, 
    inferindo automaticamente os tipos das colunas.
    """
    # Carregar o arquivo TXT para o Spark
    df = spark.read.option("delimiter", ";").csv(arquivo, header=True, inferSchema=True)
    return df


# Função para inserir dados na tabela 'atracacao_fato'
def inserir_atracacao(df_atracao: DataFrame) -> None:
    """
    Insere os dados transformados de atracação na tabela 'atracacao_fato' no SQL Server.
    
    :param df_atracao (DataFrame): DataFrame contendo os dados de atracação transformados.
    
    :return None: Não retorna nenhum valor. Apenas insere os dados no banco.
    
    A função itera sobre cada linha do DataFrame e insere os dados na tabela 'atracacao_fato',
    com base nas colunas específicas mencionadas no código.
    """
    mes_map = {
        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
    }
    
    for row in df_atracao.collect():
        # Converte as colunas de data no formato dd/mm/yyyy hh:mm:ss para datetime
        def str_to_datetime(date_str):
            return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S') if date_str else None

        data_atracacao = str_to_datetime(row['Data Atracação'])
        data_chegada = str_to_datetime(row['Data Chegada'])
        data_desatracacao = str_to_datetime(row['Data Desatracação'])
        data_inicio_operacao = str_to_datetime(row['Data Início Operação'])
        data_termino_operacao = str_to_datetime(row['Data Término Operação'])
        
        # Calculando as colunas de tempo (em dias)
        t_espera_atracacao = (data_desatracacao - data_atracacao).days if data_atracacao and data_desatracacao else None
        t_espera_inicio_op = (data_inicio_operacao - data_atracacao).days if data_atracacao and data_inicio_operacao else None
        t_operacao = (data_termino_operacao - data_inicio_operacao).days if data_inicio_operacao and data_termino_operacao else None
        t_atracado = (data_chegada - data_atracacao).days if data_chegada and data_atracacao else None
        t_estadia = (data_termino_operacao - data_chegada).days if data_chegada and data_termino_operacao else None
        
        # Convertendo Ano e Mês para formatos adequados para inserção no banco
        ano = row['Ano']  # Ano já está no formato yyyy
        mes = mes_map.get(row['Mes'].lower(), None)  # Mapeando o nome do mês para número
        
        # Executando o comando de inserção com as colunas calculadas
        cursor.execute("""
            INSERT INTO atracacao_fato
            (IDAtracacao, Tipo de Navegação da Atracação, CDTUP, Nacionalidade do Armador, IDBerco, FlagMCOperacaoAtracacao,
            Berço, Terminal, Porto Atracação, Município, Apelido Instalação Portuária, UF, Complexo Portuário, SGUF,
            Tipo da Autoridade Portuária, Região Geográfica, Data Atracação, No da Capitania, Data Chegada, No do IMO,
            Data Desatracação, TEsperaAtracacao, Data Início Operação, TesperaInicioOp, Data Término Operação, TOperacao,
            Ano da data de início da operação, TEsperaDesatracacao, Mês da data de início da operação, TAtracado,
            Tipo de Operação, TEstadia)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row['IDAtracacao'], row['Tipo de Navegação da Atracação'], row['CDTUP'], row['Nacionalidade do Armador'],
           row['IDBerco'], row['FlagMCOperacaoAtracacao'], row['Berço'], row['Terminal'], row['Porto Atracação'],
           row['Município'], row['Apelido Instalação Portuária'], row['UF'], row['Complexo Portuário'], row['SGUF'],
           row['Tipo da Autoridade Portuária'], row['Região Geográfica'], row['Data Atracação'], row['Nº da Capitania'],
           row['Data Chegada'], row['Nº do IMO'], row['Data Desatracação'], t_espera_atracacao,
           row['Data Início Operação'], t_espera_inicio_op, row['Data Término Operação'], t_operacao,
           ano, row['TEsperaDesatracacao'], mes, t_atracado, row['Tipo de Operação'], t_estadia)
        conn.commit()


def inserir_carga(df_carga: DataFrame) -> None:
    """
    Insere os dados transformados de carga na tabela 'carga_fato' no SQL Server.
    
    :param df_carga (DataFrame): DataFrame contendo os dados de carga transformados.
    
    :return None: Não retorna nenhum valor. Apenas insere os dados no banco.
    
    A função itera sobre cada linha do DataFrame e insere os dados na tabela 'carga_fato',
    com base nas colunas específicas mencionadas no código.
    """
    for row in df_carga.collect():
        # Observações sobre alguns campos:
        # Para carga conteinerizada, o valor para 'CDMercadoria' será o código das mercadorias dentro do contêiner.
        # Carga não conteinerizada = Peso bruto; Carga conteinerizada = Peso sem contêiner
        peso_liquido = row['PesoLiquidoCarga'] if row['FlagConteinerTamanho'] == 'conteinerizada' else row['PesoLiquidoCarga']
        
        cursor.execute("""
            INSERT INTO carga_fato
            (IDCarga, IDAtracacao, Origem, Destino, CDMercadoria, Tipo Operação da Carga, Carga Geral Acondicionamento,
            ConteinerEstado, Tipo Navegação, FlagAutorizacao, FlagCabotagem, FlagCabotagemMovimentacao, FlagConteinerTamanho,
            FlagLongoCurso, FlagMCOperacaoCarga, FlagOffshore, FlagTransporteViaInterioir, Percurso Transporte em vias Interiores,
            Percurso Transporte Interiores, STNaturezaCarga, STSH2, STSH4, Natureza da Carga, Sentido, TEU, QTCarga, VLPesoCargaBruta,
            Ano da data de início da operação da atracação, Mês da data de início da operação da atracação)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row['IDCarga'], row['IDAtracacao'], row['Origem'], row['Destino'], row['CDMercadoria'],
           row['Tipo Operação da Carga'], row['Carga Geral Acondicionamento'], row['ConteinerEstado'], row['Tipo Navegação'],
           row['FlagAutorizacao'], row['FlagCabotagem'], row['FlagCabotagemMovimentacao'], row['FlagConteinerTamanho'],
           row['FlagLongoCurso'], row['FlagMCOperacaoCarga'], row['FlagOffshore'], row['FlagTransporteViaInterioir'],
           row['Percurso Transporte em vias Interiores'], row['Percurso Transporte Interiores'], row['STNaturezaCarga'],
           row['STSH2'], row['STSH4'], row['Natureza da Carga'], row['Sentido'], row['TEU'], row['QTCarga'],
           row['VLPesoCargaBruta'], row['Ano da data de início da operação da atracação'], row['Mês da data de início da operação da atracação'])
        
        conn.commit()


# Carregar os arquivos TXT de Carga e Atracação
arquivos_carga = ['drive/MyDrive/Colab_Notebooks/antaq/2021Carga.txt', 'drive/MyDrive/Colab_Notebooks/antaq/2022Carga.txt', 'drive/MyDrive/Colab_Notebooks/antaq/2023Carga.txt']
arquivos_atracao = ['drive/MyDrive/Colab_Notebooks/antaq/2021Atracacao.txt', 'drive/MyDrive/Colab_Notebooks/antaq/2022Atracacao.txt', 'drive/MyDrive/Colab_Notebooks/antaq/2023Atracacao.txt']

# Carregar os dados de Carga
df_carga_2021 = carregar_arquivo_txt(arquivos_carga[0])
df_carga_2022 = carregar_arquivo_txt(arquivos_carga[1])
df_carga_2023 = carregar_arquivo_txt(arquivos_carga[2])

# Carregar os dados de Atracação
df_atracao_2021 = carregar_arquivo_txt(arquivos_atracao[0])
df_atracao_2022 = carregar_arquivo_txt(arquivos_atracao[1])
df_atracao_2023 = carregar_arquivo_txt(arquivos_atracao[2])

# Combinar os dados de todos os anos
df_carga = df_carga_2021.union(df_carga_2022).union(df_carga_2023)
df_atracao = df_atracao_2021.union(df_atracao_2022).union(df_atracao_2023)

# Chamar as funções para inserir os dados nas tabelas
inserir_atracacao(df_atracao)
inserir_carga(df_carga)

# Fechar a conexão
cursor.close()
conn.close()