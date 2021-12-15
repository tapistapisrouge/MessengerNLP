# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 23:17:11 2021

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
from package_MessengerNLP.gestion_csv import stats_occurrences

#==============================================================================================
# Si besoin de tester ligne par ligne =========================================================
#==============================================================================================
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/occurrences_graph.py'


#==============================================================================================
# Gestion chemins et infos de bases============================================================
#==============================================================================================
dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
print('Vous avez écrit : ', dossier_convers)
path_convers = pathlib.Path(__file__)
path_convers = path_convers.parent
path_convers = pathlib.Path(path_convers, 'output')
path_convers = pathlib.Path(path_convers, dossier_convers)
path_convers_csv = pathlib.Path(path_convers, 'df_messenger.csv')
print(path_convers_csv)
    
# on ouvre le dataframe
df_messenger = pd.read_csv(path_convers_csv, sep=';', decimal='.')
    
# liste des mots à chercher
list_mots = ['queinec', 'courbou', 'poste', 'merde', 'mec', 'comté', 'belfort', 'bannalec', 'fromage']

# occurrence infos
df_occurrence, df_occurrence_frequence, df_occurrence_nb = stats_occurrences.occurence(df_messenger, list_mots, col_message='message_clean_lower_ponctuation')   

# creation dossier occurrence
path_occurrences = pathlib.Path(path_convers, 'occurrences')
path_occurrences.mkdir(parents=False, exist_ok=True)

# graphique
df_occurrence_nb.set_index('participants',inplace = True)
# dt_occurrence_nb.plot.bar(rot=0)
    
# permuter le dataframe
df_test = df_occurrence_nb.T
df_test.plot.bar(rot=0)

# enregistrer
path_graph_annee = pathlib.Path(path_occurrences, 'occurrences.png')
plt.savefig(path_graph_annee)


# graphique frequence
df_occurrence_frequence.set_index('participants',inplace = True)
# dt_occurrence_nb.plot.bar(rot=0)
    
# permuter le dataframe
df_test = df_occurrence_frequence.T
df_test.plot.bar(rot=0)

# enregistrer
path_graph_annee = pathlib.Path(path_occurrences, 'occurrences_freq.png')
plt.savefig(path_graph_annee)


#==============================================================================================
# top occurrence








