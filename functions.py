import pymysql
from datetime import datetime
import pandas as pd
import numpy as np



def conectar_mysql(host, user, password, database):
    """
    Função para conectar ao banco de dados MySQL usando PyMySQL.
    """
    try:
        # Conecta ao banco de dados MySQL
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        print("Conexão estabelecida com sucesso!")
        return connection

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None


'---------------------------------------------------------------------------------------------------------------'


def formatar_data(valor):
    """
    Converte um valor de data/hora para o formato adequado (YYYY-MM-DD HH:MM:SS).
    Se o valor for inválido ou estiver ausente, retorna None.
    """
    if pd.notna(valor) and valor not in [np.nan, None, ""]:
        if isinstance(valor, str):  # Se for uma string
            try:
                return datetime.strptime(valor, "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
        elif isinstance(valor, pd.Timestamp):  # Se já for um Timestamp
            return valor.strftime("%Y-%m-%d %H:%M:%S")
    return None  # Retorna None caso o valor seja inválido ou ausente



def limpar_valor(valor):
    """
    Garante que valores NaN sejam convertidos para None antes da inserção no MySQL.
    """
    if pd.isna(valor) or valor in [np.nan, None, ""]:
        return None
    return valor



def inserir_dados(conn, dataframe, nome_tabela):
    """
    Insere os dados de um DataFrame no banco de dados MySQL.
    
    :param conn: Conexão ativa com o banco de dados.
    :param dataframe: DataFrame contendo os dados a serem inseridos.
    :param nome_tabela: Nome da tabela no banco de dados.
    """
    try:
        # Abrindo o cursor uma vez, fora do loop
        with conn.cursor() as cursor:
            for _, row in dataframe.iterrows():
                valores = []
                colunas = dataframe.columns.tolist()

                for coluna in colunas:
                    valor = row[coluna]
                    if "DATA" in coluna.upper():
                        valores.append(formatar_data(valor))
                    else:
                        valores.append(limpar_valor(valor))

                placeholders = ", ".join(["%s"] * len(colunas))
                sql = f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES ({placeholders})"
                
                # Executa a inserção para cada linha
                cursor.execute(sql, valores)
            
            # Commit após todas as inserções
            conn.commit()
            print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir dados: {e}")


'---------------------------------------------------------------------------------------------------------------'


def ler_arquivo_xlsx(caminho_arquivo):
    """
    Função para ler o arquivo Excel e retornar os dados como um DataFrame.
    """
    try:
        dados = pd.read_excel(caminho_arquivo, engine='openpyxl')
        print("Arquivo lido com sucesso!")
        return dados
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return None

