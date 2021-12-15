# -*- coding: utf-8 -*-
"""
Module stats_dataframe
--> contient des fonction qui ouvrent un dataframe d'une conversation messenger,
et qui en font des statistiques

Fonctions actuelles :
    - df_muet qui retourne un dataframe donnant le nb de jour sans parler de chaque participant pour chaque année
    - df_nb_messages qui retrounent 2 dtaframes contenant le nb de message par participants par année et par année/mois
"""

#==============================================================================================
# Modules/Packages à importer =================================================================
#==============================================================================================


import glob # pour lister les fichiers json d'un dossier en particulier
import pathlib # pour gérer les chemins
from functools import partial
import re
import json
import pandas as pd
import datetime # pour traduire timestamp en date

# packages perso
import sys
import os
# sys.path.insert(0, os.path.join(__file__, '..', '..','gestion_json'))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
print(SCRIPT_DIR)
sys.path.append(os.path.dirname(SCRIPT_DIR))

from gestion_json import base_json

# si besoin en test
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/gestion_csv/stats_dataframe.py'


#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================

def df_muet(df_messenger):
    
    """
    df_muet(df_messenger)
    --> 1 parametre : df_messenger = dataframe contenant la conversation messenger 
    --> 1 return : df_muet (dataframe, participant en y, années en x, intersection = jour sans parler)   
    """
    
    # on liste les participants
    list_participants = df_messenger['participants'].unique()
    
    # liste des années dispo dans la convers
    list_annees = list(df_messenger['annee'].unique())
    list_annees.sort()
    
    # créer le dataframe avec participants, années et stats générales 
    df_muet = pd.DataFrame(columns = ['participants'])
    for participant in list_participants:
        df_muet = df_muet.append({'participants': participant}, ignore_index=True)
    for annee in list_annees:
        df_muet[annee] = 0

    for participant in list_participants:
        df_temp = df_messenger[df_messenger['participants']==participant]     
        df_temp['diff_temps'] = 0
        df_temp = df_temp.sort_values(by=['timestamp_ms'],ascending=False)
        df_temp = df_temp.reset_index()
        
        for annee in list_annees:
            df_temp_annee = df_temp[df_temp['annee']==annee]
            df_temp_annee = df_temp_annee.sort_values(by=['timestamp_ms'],ascending=False)
            df_temp_annee = df_temp_annee.reset_index()
            
            if not df_temp_annee.empty:
                for i in range(len(df_temp_annee)-1):
                    df_temp_annee.loc[i,'diff_temps']=(df_temp_annee.loc[i,'timestamp_ms']-df_temp_annee.loc[i+1,'timestamp_ms'])
                    # on choppe les valeurs intéressantes
                    muet_sec = max(df_temp_annee['diff_temps']/1000)
                    muet_jour = muet_sec/86400
                    #ajout au dataframe de synthese
                    index_list = df_muet[df_muet['participants']==participant].index.tolist()
                    ligne = index_list[0]
                    df_muet.loc[ligne, annee] = muet_jour
            else:
                index_list = df_muet[df_muet['participants']==participant].index.tolist()
                ligne = index_list[0]
                df_muet.loc[ligne, annee] = 9999

    return(df_muet)




if __name__ == '__main__':   
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'df_messenger.csv')
    print(path_convers)
    
    # on ouvre le dataframe
    df_messenger = pd.read_csv(path_convers, sep=';', decimal='.')

    # muet infos
    df_muet = df_muet(df_messenger)


# =============================================================================================


def df_nb_messages(df_messenger):
    
    """
    df_nb_messages(df_messenger)
    --> 1 parametre : df_messenger = dataframe contenant la conversation messenger 
    --> 2 return : df_annees, df_mois_annees 
    """
    
    # on liste les participants
    list_participants = df_messenger['participants'].unique()
    # liste des années dispo dans la convers
    list_annees = list(df_messenger['annee'].unique())
    list_annees.sort()
    # list des mois
    list_mois=[i for i in range(1,13)]
    
    # créer le dataframe avec participants, années, mois et stats générales 
    df_mois_annees = pd.DataFrame(columns = ['annees', 'mois'])
    for annee in list_annees:
        for mois in list_mois:
            df_mois_annees = df_mois_annees.append({'annees': annee, 'mois': mois}, ignore_index=True)
    for participants in list_participants:
        df_mois_annees[participants] = 0
        
    # créer le dataframe avec participants, années et stats générales 
    df_annees = pd.DataFrame(columns = ['annees'])
    for annee in list_annees:
        df_annees = df_annees.append({'annees': annee}, ignore_index=True)
    for participants in list_participants:
        df_annees[participants] = 0  
              
    # compter les messages annees mois
    for participant in list_participants:
        df_temp = df_messenger[df_messenger['participants']==participant]     
        df_temp = df_temp.reset_index()

        for annee in list_annees:
            for mois in list_mois:
                df_temp_temp = df_temp[(df_temp['annee']==annee)&(df_temp['mois']==mois)] 
                if not df_temp_temp.empty:
                    nb_message = len(df_temp_temp)
                    index_list = df_mois_annees[(df_mois_annees['annees']==annee)&(df_mois_annees['mois']==mois)].index.tolist()
                    ligne = index_list[0]
                    df_mois_annees.loc[ligne, participant] = nb_message
                else:
                    index_list = df_mois_annees[(df_mois_annees['annees']==annee)&(df_mois_annees['mois']==mois)].index.tolist()
                    ligne = index_list[0]
                    df_mois_annees.loc[ligne, participant] = 0
   
    # compter les messages annees uniquement
    for participant in list_participants:
        df_temp = df_messenger[df_messenger['participants']==participant]     
        df_temp = df_temp.reset_index()

        for annee in list_annees:
            df_temp_temp = df_temp[df_temp['annee']==annee] 
            if not df_temp_temp.empty:
                nb_message = len(df_temp_temp)
                index_list = df_annees[df_annees['annees']==annee].index.tolist()
                ligne = index_list[0]
                df_annees.loc[ligne, participant] = nb_message
            else:
                index_list = df_annees[df_annees['annees']==annee].index.tolist()
                ligne = index_list[0]
                df_annees.loc[ligne, participant] = 0
        
    return(df_annees, df_mois_annees)




if __name__ == '__main__':   
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'df_messenger.csv')
    print(path_convers)
    
    # on ouvre le dataframe
    df_messenger = pd.read_csv(path_convers, sep=';', decimal='.')

    # nb messages infos
    df_annees, df_mois_annees = df_nb_messages(df_messenger)


# =============================================================================================




