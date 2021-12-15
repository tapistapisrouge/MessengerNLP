# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 14:57:00 2021

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

# packages perso
from package_MessengerNLP.gestion_json import base_json
from package_MessengerNLP.gestion_json import preparation_dataframe as prep_df

#==============================================================================================
# Si besoin de tester ligne par ligne =========================================================
#==============================================================================================
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/json_to_dataframe.py'

#==============================================================================================
# Création du dataframe =======================================================================
#==============================================================================================

dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
print('Vous avez écrit : ', dossier_convers)
path_convers = pathlib.Path(__file__)
path_convers = path_convers.parent
path_convers = pathlib.Path(path_convers, 'input')
path_convers = pathlib.Path(path_convers, dossier_convers)
print(path_convers)

# on liste les fichiers json
list_files_json = base_json.list_files_json(path_convers)

# on liste les participants
list_participants = base_json.list_participants_totale(list_files_json)
print(list_participants)

# création du dataframe ======================================================================
path_csv_output = pathlib.Path(__file__)
path_csv_output = path_csv_output.parent
path_csv_output = pathlib.Path(path_csv_output, 'output')
path_csv_output = pathlib.Path(path_csv_output, dossier_convers)
print(path_csv_output)
    
# on crée le dossier s'il n'existe pas
path_csv_output.mkdir(parents=False, exist_ok=True)
    
# on termine le nom du csv
path_csv_output = pathlib.Path(path_csv_output, "df_messenger.csv")
    
# endroit où on enregistre le dataframe
prep_df.json_to_csv(list_files_json, path_csv_output, other_participant='Diem Ngo')


#==============================================================================================
# Voir le dataframe =======================================================================
#==============================================================================================

df_messenger = pd.read_csv(path_csv_output, sep=';', decimal='.')
























