# -*- coding: utf-8 -*-
"""
Radartona Mobilab+

Criado em 10/11/2019.

Equipe:
    Ahmad
    Carolina
    Marcos

Script criado para desagrupar dados de localização de cada radar. Gera um arquivo final CSV.

"""

import psycopg2
import pandas as pd
# Disable warnings in Anaconda
import warnings
warnings.simplefilter('ignore')

try:
    connection = psycopg2.connect(user = "mobilab",
                                  password = "mobilab",
                                  host = "192.168.167.44",
                                  port = "5432",
                                  database = "hackatona")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    cursor.execute("SET search_path = radar, public;")
    cursor.execute("SELECT * FROM base_radares ORDER BY codigo LIMIT 10000;")
    record2 = cursor.fetchall()

    df2 = pd.DataFrame(record2, columns =['gid','id', 'lote', 'codigo', 'endereco','sentido','referencia','tipo_equip','enquadrame', 'qtde_fxs_f', 'data_public','velocidade','latidude_l','ligado','data_desli','motivo_desli','mi_style','mi_prix','geom','emme_gid','mdc_gid'])

    for idx in list(range(df2.shape[0])):
        df2['codigo'][idx]=df2['codigo'][idx].replace("/","-")
        lista_codigos=df2['codigo'][idx].split("-")
        if len(lista_codigos)>1:
            for idx2 in list(range(len(lista_codigos))):
                if idx2!=0:
                    df2=df2.append(df2.iloc[[idx]], ignore_index=True)
                    df2['codigo'][df2.shape[0]-1]=lista_codigos[idx2]
                    df2['codigo'][df2.shape[0]-1]=df2['codigo'][df2.shape[0]-1].replace(" ","")
                else:
                    df2['codigo'][idx]=lista_codigos[idx2]
    df2.to_csv(r'base_radares_editado.csv')

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)