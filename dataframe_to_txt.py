# -*- coding: utf-8 -*-
"""
Created on Mon Nov 22 15:50:48 2021

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

from collections import defaultdict, Counter
import markovify

# packages perso
# rien ici

#==============================================================================================
# Si besoin de tester ligne par ligne =========================================================
#==============================================================================================
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/dataframe_to_txt.py'


#==============================================================================================
# Ouverture dataframe =========================================================================
#==============================================================================================
dossier_convers = input("Entrer le nom du dossier contenant le fichier csv voulu : ")
print('Vous avez écrit : ', dossier_convers)
path_convers = pathlib.Path(__file__)
path_convers = path_convers.parent
path_convers = pathlib.Path(path_convers, 'output')
path_convers = pathlib.Path(path_convers, dossier_convers)
path_convers_csv = pathlib.Path(path_convers, 'df_messenger.csv')
print(path_convers_csv)

df_messenger = pd.read_csv(path_convers_csv, sep=';', decimal='.')


#==============================================================================================
# Fichier texte =========================================================================
#==============================================================================================

def df_to_txt(df_messenger, nom_col='message_clean', filtre_annee=False, annee_debut=None, annee_fin=None):
    print('Nombre de lignes totales : ', len(df_messenger['type']))
    
    if filtre_annee==True:
        # filtrer années annee_debut à 2021
        list_annees = list(range(annee_debut,annee_fin+1))
        df_messenger = df_messenger[df_messenger['annee'].isin(list_annees)]
        print('Nombre de lignes après le filtre années : ', len(df_messenger['type']))
    
    #filtrer sur type="Generic"
    df_messenger = df_messenger[df_messenger['type']=='Generic']
    print('Nombre de lignes Generic : ', len(df_messenger['type']))
    
    # enlever ligne vide
    df_messenger = df_messenger.dropna()
    print('Nombre de lignes non vides : ', len(df_messenger[nom_col]))
    
    df_messenger = df_messenger[df_messenger[nom_col] != '']
    print('Nombre de lignes non vides bis : ', len(df_messenger[nom_col]))
    
    # mettre message en string
    df_messenger = df_messenger.astype({nom_col: object})
    print(df_messenger.dtypes)

    # filter les lignes contenant http au debut
    # =============================================================================
    # test de la méthode
    # messages_test = ["Jay","http","https//www.wikipedia.org/valdoie", 'salut ça va https ?', 'coucou l\'ami']
    # df = pd.DataFrame(messages_test, columns = ['message'])
    # print(df)
    # mask = df['message'].str.match("[^http]")
    # df_test = df[mask]
    # print(df_test)
    # =============================================================================
    
    mask = df_messenger[nom_col].str.match("[^http]")
    df_messenger = df_messenger[mask]
    print('Nombre de lignes sans http ', len(df_messenger[nom_col]))
    
    
    # mettre en liste
    list_of_messages = df_messenger[nom_col].tolist()
    print('Nombre de messages : ', len(list_of_messages))

    # remplacement dataframe regex :
    # df.replace(to_replace=r'^ami.$', value='song', regex=True,inplace=True)
    
    #==============================================================================================
    # Création de textes ==========================================================================
    #==============================================================================================
    
    # list to str
    convers_text=" ".join(list_of_messages)
    
    # nettoyage double espace :
    convers_text = ' '.join(convers_text.split())
    
    return(convers_text)

#=====================================================================================================
nom_col = 'message_clean_lower_ponctuation' 
annee_debut = 2018
annee_fin = 2020

convers_text = df_to_txt(df_messenger, nom_col=nom_col, filtre_annee=True, annee_debut=annee_debut, annee_fin=annee_fin)


#=====================================================================================================
# enregistrement en txt
nom_fichier = 'df_messenger_reduit.txt'
path_convers_txt = pathlib.Path(path_convers, nom_fichier)
print(path_convers_txt)

with open(path_convers_txt, "a") as fichier:
    fichier.write(convers_text)