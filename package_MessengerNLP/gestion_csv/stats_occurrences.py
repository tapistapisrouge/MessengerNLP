# -*- coding: utf-8 -*-
"""
@author: amaur
Module de calcul d'occurrence de mot depuis le fichier csv d'une convers Messenger
Fonctions :
    - occurrence : nb occurrence et fréquence d'occurrence
    - occurrence_annees : à venir, idem occurrence avec information par années
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
# import base_json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
print(SCRIPT_DIR)
sys.path.append(os.path.dirname(SCRIPT_DIR))

from gestion_json import base_json

# si besoin en test
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/gestion_csv/occurrences.py'


#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================

def occurence(df_messenger, list_mots, col_message):
    """
    Parameters
    ----------
    df_messenger : pandas dataframe
        dataframe contenant une colonne 'participants' et 'message'.
    list_mots : List
        liste des mots à chercher dans la conversation.
    col_message : character
        Nom de la colonne contenant les messages où chercher les occurrences.
    """
    # petit nettoyage dt_messenger
    df_messenger = df_messenger[df_messenger['type']=='Generic']
    df_messenger.dropna(subset = [col_message], inplace=True)
    
    # on liste les participants
    list_participants = df_messenger['participants'].unique()
    
    # liste des années dispo dans la convers
    # list_annees = list(dt_messenger['annee'].unique())
    # list_annees.sort()
    
    # créer le dataframe
    df_occurrence = pd.DataFrame(list_participants, columns = ['participants'])
    df_occurrence['nb_messages'] = 0 
    for mot in list_mots:
        df_occurrence[mot] = 0  
    
    # boucle sur les gens
    for participant in list_participants:
        # mettre le nom de message pour calculer les fréquences plus tard
        df_temp = df_messenger[df_messenger['participants']==participant]
        nb_message = len(df_temp)
        index_list = df_occurrence[df_occurrence['participants']==participant].index.tolist()
        ligne = index_list[0]       
        df_occurrence.loc[ligne,'nb_messages'] = nb_message
        
        # recherche des occurrences
        for mot in list_mots:
            count=0
            #boucle sur les mots recherchés
            for message in df_temp[col_message]:
                if type(message)!=str:
                    message = str(message)
                occurrence = message.count(mot)
                count+=occurrence
            # ajouter le total au dataframe
            index_list = df_occurrence[df_occurrence['participants']==participant].index.tolist()
            ligne = index_list[0]
            df_occurrence.loc[ligne,mot] = count
 
    # ajouter les colonnes frequences
    list_colonnes = df_occurrence.columns
    list_colonnes = list_colonnes[2:len(list_colonnes)]
    for colonne in list_colonnes:
        nom_col = colonne+"_%"
        print(nom_col)
        df_occurrence[nom_col] = df_occurrence[colonne]/df_occurrence['nb_messages']*10000
    
    # dataframe reduit en ne gardant que les %
    colonne_finale = len(list_mots)+2
    df_occurrence_frequence = df_occurrence.drop(df_occurrence.iloc[:,1:colonne_finale],1,inplace=False)
    
    # dataframe nb
    df_occurrence_nb = df_occurrence.drop(df_occurrence.iloc[:,colonne_finale:],1,inplace=False)
    df_occurrence_nb.drop(columns=["nb_messages"],inplace=True)
    
    return[df_occurrence, df_occurrence_frequence, df_occurrence_nb]    
    
    
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
    
    # liste des mots à chercher
    # ['c\'est', 'je', 'moi', 'pourquoi', 'comment', 'possible', ' vie ']
    # ['pourquoi', 'comment', 'quand', 'où', 'qui', 'quoi', 'combien']
    # ['enfant', 'adulte', 'voilà', 'riz', ' chat ']
    list_mots = ['dieu', ' nutrition', 'volonté', 'courage', 'poids']

    # muet infos
    df_occurrence, df_occurrence_frequence, df_occurrence_nb = occurence(df_messenger, list_mots, col_message='message_clean_lower_ponctuation')   


    # graphique
    df_occurrence_nb.set_index('participants',inplace = True)
    # dt_occurrence_nb.plot.bar(rot=0)
    
    # permuter le dataframe
    df_test = df_occurrence_nb.T
    df_test.plot.bar(rot=0)















    