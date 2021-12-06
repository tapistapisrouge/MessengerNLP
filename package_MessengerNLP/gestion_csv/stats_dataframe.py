# -*- coding: utf-8 -*-
"""
Created on Mon Nov 15 00:19:27 2021

@author: amaur
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

def dt_muet(dt_messenger):
    
    # on liste les participants
    list_participants = dt_messenger['participants'].unique()
    
    # liste des années dispo dans la convers
    list_annees = list(dt_messenger['annee'].unique())
    list_annees.sort()
    
    # créer le dataframe avec participants, années et stats générales 
    dt_muet = pd.DataFrame(columns = ['participants'])
    for participant in list_participants:
        dt_muet = dt_muet.append({'participants': participant}, ignore_index=True)
    for annee in list_annees:
        dt_muet[annee] = 0

    for participant in list_participants:
        dt_temp = dt_messenger[dt_messenger['participants']==participant]     
        dt_temp['diff_temps'] = 0
        dt_temp = dt_temp.sort_values(by=['timestamp_ms'],ascending=False)
        dt_temp = dt_temp.reset_index()
        
        for annee in list_annees:
            dt_temp_annee = dt_temp[dt_temp['annee']==annee]
            dt_temp_annee = dt_temp_annee.sort_values(by=['timestamp_ms'],ascending=False)
            dt_temp_annee = dt_temp_annee.reset_index()
            
            if not dt_temp_annee.empty:
                for i in range(len(dt_temp_annee)-1):
                    dt_temp_annee.loc[i,'diff_temps']=(dt_temp_annee.loc[i,'timestamp_ms']-dt_temp_annee.loc[i+1,'timestamp_ms'])
                    # on choppe les valeurs intéressantes
                    muet_sec = max(dt_temp_annee['diff_temps']/1000)
                    muet_jour = muet_sec/86400
                    #ajout au dataframe de synthese
                    index_list = dt_muet[dt_muet['participants']==participant].index.tolist()
                    ligne = index_list[0]
                    dt_muet.loc[ligne, annee] = muet_jour
            else:
                index_list = dt_muet[dt_muet['participants']==participant].index.tolist()
                ligne = index_list[0]
                dt_muet.loc[ligne, annee] = 9999

    return(dt_muet)




if __name__ == '__main__':   
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'dt_messenger.csv')
    print(path_convers)
    
    # on ouvre le dataframe
    dt_messenger = pd.read_csv(path_convers, sep=';', decimal='.')

    # muet infos
    dt_muet = dt_muet(dt_messenger)


# =============================================================================================


def dt_nb_messages(dt_messenger):
    # on liste les participants
    list_participants = dt_messenger['participants'].unique()
    # liste des années dispo dans la convers
    list_annees = list(dt_messenger['annee'].unique())
    list_annees.sort()
    # list des mois
    list_mois=[i for i in range(1,13)]
    
    # créer le dataframe avec participants, années, mois et stats générales 
    dt_mois_annees = pd.DataFrame(columns = ['annees', 'mois'])
    for annee in list_annees:
        for mois in list_mois:
            dt_mois_annees = dt_mois_annees.append({'annees': annee, 'mois': mois}, ignore_index=True)
    for participants in list_participants:
        dt_mois_annees[participants] = 0
        
    # créer le dataframe avec participants, années et stats générales 
    dt_annees = pd.DataFrame(columns = ['annees'])
    for annee in list_annees:
        dt_annees = dt_annees.append({'annees': annee}, ignore_index=True)
    for participants in list_participants:
        dt_annees[participants] = 0  
              
    # compter les messages annees mois
    for participant in list_participants:
        dt_temp = dt_messenger[dt_messenger['participants']==participant]     
        dt_temp = dt_temp.reset_index()

        for annee in list_annees:
            for mois in list_mois:
                dt_temp_temp = dt_temp[(dt_temp['annee']==annee)&(dt_temp['mois']==mois)] 
                if not dt_temp_temp.empty:
                    nb_message = len(dt_temp_temp)
                    index_list = dt_mois_annees[(dt_mois_annees['annees']==annee)&(dt_mois_annees['mois']==mois)].index.tolist()
                    ligne = index_list[0]
                    dt_mois_annees.loc[ligne, participant] = nb_message
                else:
                    index_list = dt_mois_annees[(dt_mois_annees['annees']==annee)&(dt_mois_annees['mois']==mois)].index.tolist()
                    ligne = index_list[0]
                    dt_mois_annees.loc[ligne, participant] = 0
   
    # compter les messages annees uniquement
    for participant in list_participants:
        dt_temp = dt_messenger[dt_messenger['participants']==participant]     
        dt_temp = dt_temp.reset_index()

        for annee in list_annees:
            dt_temp_temp = dt_temp[dt_temp['annee']==annee] 
            if not dt_temp_temp.empty:
                nb_message = len(dt_temp_temp)
                index_list = dt_annees[dt_annees['annees']==annee].index.tolist()
                ligne = index_list[0]
                dt_annees.loc[ligne, participant] = nb_message
            else:
                index_list = dt_annees[dt_annees['annees']==annee].index.tolist()
                ligne = index_list[0]
                dt_annees.loc[ligne, participant] = 0
        
    return(dt_annees, dt_mois_annees)




if __name__ == '__main__':   
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'dt_messenger.csv')
    print(path_convers)
    
    # on ouvre le dataframe
    dt_messenger = pd.read_csv(path_convers, sep=';', decimal='.')

    # nb messages infos
    dt_annees, dt_mois_annees = dt_nb_messages(dt_messenger)


# =============================================================================================




