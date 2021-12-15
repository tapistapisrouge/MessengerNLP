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
from package_MessengerNLP.gestion_csv import df_to_txt

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

# fichier texte général

nom_col = 'message_clean_lower_ponctuation' 
annee_debut = 2018
annee_fin = 2020

# convers_text = df_to_txt(df_messenger, nom_col=nom_col, filtre_annee=True, annee_debut=annee_debut, annee_fin=annee_fin, separateur = ' ')
convers_text = df_to_txt.df_to_txt(df_messenger, nom_col=nom_col)

# fichier texte par participants

dict_convers_text = df_to_txt.df_to_txt_participants(df_messenger, nom_col=nom_col, 
                                                filtre_annee=False, annee_debut=None, annee_fin=None, separateur=' ')


#=====================================================================================================
# enregistrement en txt
nom_fichier = 'df_messenger.txt'

# on crée le dossier txt
path_txt = pathlib.Path(path_convers, 'txt')
path_txt.mkdir(parents=False, exist_ok=True)

path_convers_txt = pathlib.Path(path_txt, nom_fichier)
print(path_convers_txt)

with open(path_convers_txt, "a") as fichier:
    fichier.write(convers_text)
    

# enregistrement de chaque participant
for cle,valeur in dict_convers_text.items():
    participant = str(cle).lower()
    participant = ''.join(participant.split())
    print(participant)
    conversation = str(valeur)
    nom_fichier_participant = participant+".txt"
    path_convers_txt = pathlib.Path(path_txt, nom_fichier_participant)
    with open(path_convers_txt, "a") as fichier:
        fichier.write(conversation)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
