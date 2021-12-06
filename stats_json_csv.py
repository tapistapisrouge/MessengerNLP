# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 10:39:45 2021

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
import matplotlib.pyplot as plt
import numpy as np

# packages perso
from package_MessengerNLP.gestion_json import base_json, stats_json
from package_MessengerNLP.gestion_csv import stats_dataframe as stats_df

#==============================================================================================
# Si besoin de tester ligne par ligne =========================================================
#==============================================================================================
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/stats_json_csv.py'


#==============================================================================================
# Gestion chemins et infos de bases============================================================
#==============================================================================================

#==============================================================================================
dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
print('Vous avez écrit : ', dossier_convers)
path_convers = pathlib.Path(__file__)
path_convers = path_convers.parent

# chemin des input : fichiers json pour stats json
path_convers_input = pathlib.Path(path_convers, 'input')
path_convers_input = pathlib.Path(path_convers_input, dossier_convers)
print(path_convers_input)

# chemin des output : fichiers csv à récupérer
path_convers_output = pathlib.Path(path_convers, 'output')
path_convers_output = pathlib.Path(path_convers_output, dossier_convers)
print(path_convers_output)

# recup dataframe
path_convers_messenger = pathlib.Path(path_convers_output, 'dt_messenger.csv')
dt_messenger = pd.read_csv(path_convers_messenger, sep=';', decimal='.')

#==============================================================================================
# infos de base

# on liste les fichiers json
list_files_json = base_json.list_files_json(path_convers_input)
print(list_files_json)
    
# on liste les participants
list_participants = base_json.list_participants_totale(list_files_json)
print(list_participants)
    
# liste des années dispo dans la convers
list_annees = base_json.list_annees(list_files_json)
print(list_annees)
    
# liste des clés existante dans les messages
list_cles = base_json.list_cles_messages(list_files_json)
print(list_cles)

# on liste les reactions
list_reactions = base_json.list_reactions(list_files_json)   


#==============================================================================================
# Stats et graphiques =========================================================================
#==============================================================================================

# création dossier stats_json et stats_csv
# If parents is false (the default), a missing parent raises FileNotFoundError.
# If exist_ok is false (the default), FileExistsError is raised if the target directory already exists.
path_stats_csv = pathlib.Path(path_convers_output, 'stats_csv')
path_stats_csv.mkdir(parents=True, exist_ok=True)
path_stats_json = pathlib.Path(path_convers_output, 'stats_json')
path_stats_json.mkdir(parents=True, exist_ok=True)

# stats csv

#muet
dt_muet = stats_df.dt_muet(dt_messenger)
path_muet = pathlib.Path(path_stats_csv, "dt_muet.csv")
dt_muet.to_csv(path_muet, encoding='utf-8-sig', sep=';', decimal='.', index=False)

#nb_message
dt_annees, dt_annees_mois = stats_df.dt_nb_messages(dt_messenger)
path_annees = pathlib.Path(path_stats_csv, "dt_annees.csv")
path_annees_mois = pathlib.Path(path_stats_csv, "dt_annees_mois.csv")

dt_annees.to_csv(path_annees, encoding='utf-8-sig', sep=';', decimal='.', index=False)
dt_annees_mois.to_csv(path_annees_mois, encoding='utf-8-sig', sep=';', decimal='.', index=False)   

#==========================================================================================
# graphique

# annees uniquement =======================================================================
dt_annees.dtypes
dt_annees['annees']= dt_annees['annees'].apply(np.int64)
dt_annees = dt_annees.sort_values('annees', ascending=True)
dt_annees['annees'] =  pd.to_datetime(dt_annees['annees'], format='%Y')

list_col = list(dt_annees.columns)
list_col.remove('annees')
print(list_col)

list_couleur = ['#0e670c','#05a1d1' ,'#e00dd7' ,'#d17a05' ,'#75de28', '#1705d1','#010200', '#eb1515']

plt.figure(figsize=(20,12))
for col in list_col:
    plt.plot(dt_annees['annees'], dt_annees[col], color=list_couleur[0], linewidth=3, label=col)
    del list_couleur[0]    
# suite
plt.xlabel('Années',fontsize=22)
plt.ylabel('Nb messages',fontsize=22)
plt.xticks(fontsize=20)
#plt.xticks(rotation=45)
#-plt.xticks(rotation='vertical')
plt.yticks(fontsize=20)
plt.title('Evolution du bavardage messenger',fontsize=30)
plt.legend(fontsize=20)

#enregistrer
path_graph_annee = pathlib.Path(path_stats_csv, 'nb_message_annees.png')
plt.savefig(path_graph_annee)



# annees et mois =======================================================================
dt_annees_mois['annees']= dt_annees_mois['annees'].apply(str)
dt_annees_mois['mois']= dt_annees_mois['mois'].apply(str)
print(dt_annees_mois.dtypes)

dt_annees_mois["annees-mois"] = dt_annees_mois[['annees', 'mois']].apply('-'.join, axis=1)

dt_annees_mois['annees']= dt_annees_mois['annees'].apply(np.int64)
dt_annees_mois['mois']= dt_annees_mois['mois'].apply(np.int64)
dt_annees_mois = dt_annees_mois.sort_values(['annees', 'mois'], ascending=[True, True])
dt_annees_mois.dtypes

# dt_annees_mois['annees-mois'] =  pd.to_datetime(dt_annees_mois['annees-mois'], format='%Y-%m')

list_col = list(dt_annees_mois.columns)
list_col.remove('annees')
list_col.remove('mois')
list_col.remove('annees-mois')
print(list_col)

list_couleur = ['#0e670c','#05a1d1' ,'#e00dd7' ,'#d17a05' ,'#75de28', '#1705d1','#010200', '#eb1515']

plt.figure(figsize=(20,12))
for col in list_col:
    plt.plot(dt_annees_mois['annees-mois'], dt_annees_mois[col], color=list_couleur[0], linewidth=3, label=col)
    del list_couleur[0]    
# suite
plt.xlabel('Années',fontsize=22)
plt.ylabel('Nb messages',fontsize=22)
plt.xticks(fontsize=20, rotation=60)
#plt.xticks(rotation=45)
#-plt.xticks(rotation='vertical')
plt.yticks(fontsize=20)
plt.title('Evolution du bavardage messenger',fontsize=30)
plt.legend(fontsize=20)

#enregistrer
path_graph_annee_mois = pathlib.Path(path_stats_csv, 'nb_message_annees_mois.png')
plt.savefig(path_graph_annee_mois)



