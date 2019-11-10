# -*- coding: utf-8 -*-
"""
Radartona Mobilab+

Criado em 10/11/2019.

Equipe:
    Ahmad
    Carolina
    Marcos
"""

# Importação de bibliotecas
import psycopg2
import numpy as np
import pandas as pd
# Disable warnings in Anaconda
import warnings
warnings.simplefilter('ignore')
# We will display plots right inside Jupyter Notebook
#matplotlib inline
import matplotlib.pyplot as plt
# We will use the Seaborn library
import seaborn as sns
sns.set()
# Graphics in SVG format are more sharp and legible
#config InlineBackend.figure_format = 'svg'
# Increase the default plot size
from pylab import rcParams
# import gmplot package 
import gmplot
_, axes = plt.subplots(1, 2, sharey=True, figsize=(6, 4))

rcParams['figure.figsize'] = 5, 4

# Conexão ao banco de dados
try:
    connection = psycopg2.connect(user = "mobilab",
                                  password = "mobilab",
                                  host = "192.168.167.44",
                                  port = "5432",
                                  database = "hackatona")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SET search_path = radar, public;")
    cursor.execute("SELECT to_char(data_e_hora, 'HH24') as hora_do_dia,sum(contagem),sum(placas),sum(placas)*100/sum(contagem),localidade,tipo FROM contagens WHERE to_char(data_e_hora, 'YYYY')='2018' GROUP BY hora_do_dia, localidade, tipo ORDER BY hora_do_dia LIMIT 1000000;")
    record = cursor.fetchall()

# TABELAS DE RESULTADOS PARA ANALISE
    
# Base de dados - Contagens
    df = pd.DataFrame(record, columns =['hora_do_dia', 'contagem', 'placas','indice','codigo','tipo'])
    df['hora_do_dia']=df['hora_do_dia'].astype(str).astype(int)

# Base de dados - Parâmetros dos radares
    df2 = pd.read_csv('base_radares_editado.csv')
    df2['codigo']=df2['codigo'].astype(str).astype(int)
    df3 = df.merge(df2, on='codigo', how='left')

# Informações das colunas
    print(df.info())
    print(df2.info())
    grouping_columns=['hora_do_dia','tipo_equip']

# Tipo equipamento
    df4=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df4['indice'] = np.divide(df4['placas'],df4['contagem'])

# Fornecedor
    grouping_columns=['hora_do_dia','lote']
    df5=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df5['indice'] = np.divide(df5['placas'],df5['contagem'])

# Tipo de veículo e tipo de radar
    grouping_columns=['hora_do_dia','lote','tipo']
    df6=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df6['indice'] = np.divide(df6['placas'],df6['contagem'])
    
# Tipo de veículo
    grouping_columns=['hora_do_dia','tipo']
    df7=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df7['indice'] = np.divide(df7['placas'],df7['contagem'])
    
# Localização
    grouping_columns=['codigo','latidude_l']
    df8=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df8['indice'] = np.divide(df8['placas'],df8['contagem'])

# Localização e tipo
    grouping_columns=['codigo','tipo_equip']
    df9=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df9['indice'] = np.divide(df9['placas'],df9['contagem'])
    
# Fornecedor e tipo de radar
    grouping_columns=['codigo','lote','tipo_equip']
    df10=df3.groupby(grouping_columns, as_index=False).agg({"contagem": "sum","placas": "sum"})
    df10['indice'] = np.divide(df10['placas'],df10['contagem'])
    grouping_columns=['lote','tipo_equip']
    df11=df10.groupby(grouping_columns, as_index=False).agg({"codigo":"count","contagem": "sum","placas": "sum"})
    df11['indice'] = np.divide(df11['placas'],df11['contagem'])

# Processamento dos dados de latitude e longitude
    latitude_list=[]
    longitude_list=[]
    for idx in list(range(df8.shape[0])):
        if df8['indice'][idx]<=1.0:         # Índice de leitura mínimo
            if not pd.isnull(df8['latidude_l'][idx]):
                if df8['latidude_l'][idx].find(' ')!=-1:
                    replaced_lat=df8['latidude_l'][idx].replace("(","")
                    replaced_lat=replaced_lat.replace(")","")
                    latlong1=float(replaced_lat.split()[0])
                    latlong2=float(replaced_lat.split()[1])
                    if latlong1>-30:
                        latitude_list.append(latlong1) 
                        longitude_list.append(latlong2) 
                    else:
                        latitude_list.append(latlong2) 
                        longitude_list.append(latlong1) 

# Plotagem da localização dos radares com índice inferior a X
    gmap3 = gmplot.GoogleMapPlotter(-23.544183, -46.636198,12) 
    gmap3.scatter( latitude_list, longitude_list, 'r', 
                              size = 100, marker = False ) 
    gmap3.draw( "map100.html" ) 

# Estatística de distribuição por índice
    features = ['indice']
    sns.boxplot(data=df9[features], ax=axes[0]); 
    sns.violinplot(data=df9[features], ax=axes[1]);  

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")