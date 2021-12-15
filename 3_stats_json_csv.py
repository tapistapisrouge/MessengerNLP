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
path_convers_messenger = pathlib.Path(path_convers_output, 'df_messenger.csv')
df_messenger = pd.read_csv(path_convers_messenger, sep=';', decimal='.')

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
path_stats_csv.mkdir(parents=False, exist_ok=True)
path_stats_json = pathlib.Path(path_convers_output, 'stats_json')
path_stats_json.mkdir(parents=False, exist_ok=True)


# stats json =========================================================================

# stats globales
df_stats_globales, df_stats_globales_pourcentage, df_stats_globales_group, df_stats_globales_pourcentage_group = stats_json.json_stats_globales(list_files_json, list_participants, list_annees, list_cles, other_participant=False)

path_df_stats_globales = pathlib.Path(path_stats_json, "df_stats_globales.csv")
df_stats_globales.to_csv(path_df_stats_globales, encoding='utf-8-sig', sep=';', decimal='.', index=False)

path_df_stats_globales_pourcentage = pathlib.Path(path_stats_json, "df_stats_globales_pourcentage.csv")
df_stats_globales_pourcentage.to_csv(path_df_stats_globales_pourcentage, encoding='utf-8-sig', sep=';', decimal='.', index=False)

path_df_stats_globales_group = pathlib.Path(path_stats_json, "df_stats_globales_group.csv")
df_stats_globales_group.to_csv(path_df_stats_globales_group, encoding='utf-8-sig', sep=';', decimal='.', index=False)

path_df_stats_globales_pourcentage_group = pathlib.Path(path_stats_json, "df_stats_globales_pourcentage_group.csv")
df_stats_globales_pourcentage_group.to_csv(path_df_stats_globales_pourcentage_group, encoding='utf-8-sig', sep=';', decimal='.', index=False)

# reactions
df_reactions, df_reactions_group = stats_json.reaction_infos(list_files_json, list_participants, list_annees, list_reactions, other_participant=False)
    
path_df_reactions = pathlib.Path(path_stats_json, "df_reactions.csv")
df_reactions.to_csv(path_df_reactions, encoding='utf-8-sig', sep=';', decimal='.', index=False)

path_df_reactions_group = pathlib.Path(path_stats_json, "df_reactions_group.csv")
df_reactions_group.to_csv(path_df_reactions_group, encoding='utf-8-sig', sep=';', decimal='.', index=False)

#kick
df_kick, df_kick_group = stats_json.kick_infos(list_files_json, list_participants, list_annees, other_participant=False)

path_df_kick = pathlib.Path(path_stats_json, "df_kick.csv")
df_kick.to_csv(path_df_kick, encoding='utf-8-sig', sep=';', decimal='.', index=False)

path_df_kick_group = pathlib.Path(path_stats_json, "df_kick_group.csv")
df_kick_group.to_csv(path_df_kick_group, encoding='utf-8-sig', sep=';', decimal='.', index=False)


# stats csv ==================================================================

#muet
df_muet = stats_df.df_muet(df_messenger)
path_muet = pathlib.Path(path_stats_csv, "df_muet.csv")
df_muet.to_csv(path_muet, encoding='utf-8-sig', sep=';', decimal='.', index=False)

#nb_message
df_annees, df_annees_mois = stats_df.df_nb_messages(df_messenger)
path_annees = pathlib.Path(path_stats_csv, "df_annees.csv")
path_annees_mois = pathlib.Path(path_stats_csv, "df_annees_mois.csv")

df_annees.to_csv(path_annees, encoding='utf-8-sig', sep=';', decimal='.', index=False)
df_annees_mois.to_csv(path_annees_mois, encoding='utf-8-sig', sep=';', decimal='.', index=False)   

#==========================================================================================
# graphique

# annees uniquement =======================================================================
df_annees.dtypes
df_annees['annees']= df_annees['annees'].apply(np.int64)
df_annees = df_annees.sort_values('annees', ascending=True)
df_annees['annees'] =  pd.to_datetime(df_annees['annees'], format='%Y')

list_col = list(df_annees.columns)
list_col.remove('annees')
print(list_col)

list_couleur = ['#0e670c','#05a1d1' ,'#e00dd7' ,'#d17a05' ,'#75de28', '#1705d1','#010200', '#eb1515']

plt.figure(figsize=(20,12))
for col in list_col:
    plt.plot(df_annees['annees'], df_annees[col], color=list_couleur[0], linewidth=3, label=col)
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
df_annees_mois['annees']= df_annees_mois['annees'].apply(str)
df_annees_mois['mois']= df_annees_mois['mois'].apply(str)
print(df_annees_mois.dtypes)

df_annees_mois["annees-mois"] = df_annees_mois[['annees', 'mois']].apply('-'.join, axis=1)

df_annees_mois['annees']= df_annees_mois['annees'].apply(np.int64)
df_annees_mois['mois']= df_annees_mois['mois'].apply(np.int64)
df_annees_mois = df_annees_mois.sort_values(['annees', 'mois'], ascending=[True, True])
df_annees_mois.dtypes

# dt_annees_mois['annees-mois'] =  pd.to_datetime(dt_annees_mois['annees-mois'], format='%Y-%m')

list_col = list(df_annees_mois.columns)
list_col.remove('annees')
list_col.remove('mois')
list_col.remove('annees-mois')
print(list_col)

list_couleur = ['#0e670c','#05a1d1' ,'#e00dd7' ,'#d17a05' ,'#75de28', '#1705d1','#010200', '#eb1515']

plt.figure(figsize=(20,12))
for col in list_col:
    plt.plot(df_annees_mois['annees-mois'], df_annees_mois[col], color=list_couleur[0], linewidth=3, label=col)
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



