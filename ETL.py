#!/usr/bin/env python
# coding: utf-8

# In[39]:


#Importando as Bibliotecas
import pandas as pd


# In[40]:



def read_files(path,name_file, year_date, type_file):
    
    _file = f'{path}{name_file}{year_date}.{type_file}'
    
    colspecs = [(2,10),
                (10,12),
                (12,24),
                (27,39),
                (56,69),
                (69,82),
                (82,95),
                (108,121),
                (152,170),
                (170,188)
    ]

    names = ['data_pregao','codbdi','sigla_acao','nome_acao','preco_abertura','preco_maximo','preco_minimo','preco_fechamento','qtd_negocios','volume_negocios']

    df = pd.read_fwf(_file, colspecs = colspecs, names = names, skiprows = 1)
    
    return df


# In[41]:


#Filtrar Ações

def filter_stoks(df):
    df = df [df['codbdi']==2]
    df = df.drop(['codbdi'],1)
    
    return df


# In[42]:


#Ajuste campo de data

def parse_date(df):
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format = '%Y%m%d')
    return df


# In[43]:


#Ajuste dos campos numéricos

def parse_values(df):

    df['preco_abertura'] = (df['preco_abertura'] / 100).astype(float)
    df['preco_maximo'] = (df['preco_maximo'] / 100).astype(float)
    df['preco_minimo'] = (df['preco_minimo'] / 100).astype(float)
    df['preco_fechamento'] = (df['preco_fechamento'] / 100).astype(float)
    df['qtd_negocios'] = (df['qtd_negocios']).astype(int)
    df['volume_negocios'] = (df['volume_negocios']).astype(int)
    
    return df


# In[44]:


#Juntando os Arquivos

def concat_files(path, name_file, year_date, type_file, final_file):
    
    for i , y in enumerate(year_date):
        df = read_files(path, name_file, y, type_file)
        df = filter_stoks(df)
        df = parse_date(df)
        df = parse_values(df)
        
        if i==0:
            df_final = df
        else:
            df_final =pd.concat([df_final, df])
            
        df_final.to_csv(f'{path}//{final_file}', index = False)


# In[45]:


#Executando programa de ETL

year_date = ['2020','2021','2022']

path=f'C://Users//dleye//Documents//Programa_de_Manipulação_de_Dados//'

name_file = 'COTAHIST_A'

type_file = 'txt'

final_file = 'all_bovespa.csv'

concat_files(path, name_file, year_date,type_file, final_file)


# In[ ]:




