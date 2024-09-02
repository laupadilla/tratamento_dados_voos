import sqlite3
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime
import logging

load_dotenv()

def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    
    logger.info(f'Iniciando o saneamento dos dados ; {datetime.datetime.now()}')
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def feat_eng(df):
    '''
    Função que realiza a criação de novos campos no DataFrame
    INPUT: Pandas DataFrame contendo os dados originais
    OUTPUT: Pandas DataFrame contendo os dados originais e os dados novos
    '''
    logging.info(f"Criando novas colunas no dataframe. {datetime.datetime.now()}")

    df["tempo_voo_esperado"] = (df["datetime_chegada_formatted"] - df["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
    df["tempo_voo_hr"] = df["tempo_voo"] /60
    df["atraso"] = df["tempo_voo_hr"] - df["tempo_voo_esperado"]
    df["dia_semana"] = df["data_voo"].dt.day_of_week
    df["horario"] = df.loc[:,"datetime_partida_formatted"].dt.hour.apply(lambda x: classifica_hora(x))

    logging.info(f"Novas colunas criadas com sucesso! {datetime.datetime.now()}")

    return df

def classifica_hora(hra):
    if 0 <= hra < 6: return "MADRUGADA"
    elif 6 <= hra < 12: return "MANHA"
    elif 12 <= hra < 18: return "TARDE"
    else: return "NOITE"

def save_data_sqlite(df):
    '''
    Função que salva os dados no banco de dados
    INPUT: Pandas DataFrame
    '''

    logger.info(f'Salvando os dados no banco de dados; {datetime.datetime.now()}')
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):
    '''
    Função que faz uma query dos dados no banco de dados
    INPUT: Tabela de metadados
    OUTPUT: Print dos dados
    '''
    logger.info(f'Buscando dados no banco de dados; {datetime.datetime.now()}')
    try:
        conn = sqlite3.connect("data/NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado('assets/work_metadado_flights.xlsx')
    df = pd.read_csv(('https://raw.githubusercontent.com/JackyP/testing/master/datasets/nycflights.csv'),index_col=0)
    df = data_clean(df, metadados)
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves_renamed"])
    df = feat_eng(df)
    save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')