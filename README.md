# ETL de Dados Portuários com PySpark e SQL Server

## Descrição
Este projeto implementa um processo ETL (Extract, Transform, Load) utilizando **PySpark** para processar dados portuários armazenados em arquivos TXT e inseri-los em um banco de dados **SQL Server**. O objetivo é facilitar a análise e gestão de dados de atracação e carga de portos brasileiros.

## Tecnologias Utilizadas
- **Python 3.12.2**
- **PySpark** (para processamento distribuído de dados)
- **pyodbc** (para conexão com SQL Server)
- **dotenv** (para carregamento de variáveis de ambiente)
- **SQL Server** (armazenamento dos dados processados)

## Configuração do Ambiente
Antes de executar o projeto, certifique-se de configurar as seguintes variáveis de ambiente no arquivo `.env`:
```ini
DRIVER_NAME=Nome_do_Driver_SQL_Server
SERVER_NAME=Nome_do_Servidor
DATABASE_NAME=Nome_do_Banco_de_Dados
TRUSTED_CONNECTION=yes
```

## Instalação das Dependências
Instale as dependências necessárias utilizando o pip:
```bash
pip install pyspark pyodbc python-dotenv
```

## Estrutura do Código
O código está estruturado em funções modulares para facilitar a reutilização e a manutenção.

### 1. Carregamento do Arquivo TXT
A função **carregar_arquivo_txt** carrega um arquivo TXT delimitado por ponto e vírgula (;) e transforma os dados em um **DataFrame do Spark**.
```python
def carregar_arquivo_txt(arquivo: str) -> DataFrame:
    df = spark.read.option("delimiter", ";").csv(arquivo, header=True, inferSchema=True)
    return df
```

### 2. Inserção de Dados na Tabela `atracacao_fato`
A função **inserir_atracacao** processa os dados de atracação e os insere na tabela `atracacao_fato` do SQL Server.
```python
def inserir_atracacao(df_atracao: DataFrame) -> None:
    for row in df_atracao.collect():
        cursor.execute("INSERT INTO atracacao_fato (...) VALUES (?, ?, ...)", ...)
        conn.commit()
```

### 3. Inserção de Dados na Tabela `carga_fato`
A função **inserir_carga** processa os dados de carga e os insere na tabela `carga_fato` do SQL Server.
```python
def inserir_carga(df_carga: DataFrame) -> None:
    for row in df_carga.collect():
        cursor.execute("INSERT INTO carga_fato (...) VALUES (?, ?, ...)", ...)
        conn.commit()
```

## Execução do ETL
1. Carregue os dados a partir de um arquivo TXT:
   ```python
   df_atracao = carregar_arquivo_txt("dados_atracao.txt")
   df_carga = carregar_arquivo_txt("dados_carga.txt")
   ```

2. Insira os dados processados no SQL Server:
   ```python
   inserir_atracacao(df_atracao)
   inserir_carga(df_carga)
   ```

## Observações
- Certifique-se de que o banco de dados **SQL Server** esteja configurado corretamente e acessível.
- Verifique se as tabelas **atracacao_fato** e **carga_fato** já estão criadas no banco de dados.
- Os dados são transformados antes da inserção para garantir consistência e formato adequado.

## Autor
Desenvolvido por Cainã Moura.

