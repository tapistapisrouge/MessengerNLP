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
path_convers = pathlib.Path(path_convers, 'dt_messenger.csv')
print(path_convers)
    
# on ouvre le dataframe
dt_messenger = pd.read_csv(path_convers, sep=';', decimal='.')
    
# liste des mots à chercher
list_mots = ['impeccable', 'futur', 'présent', 'passé', 'avenir', 'dispo']

# occurrence infos
# dt_occurrence, dt_occurrence_frequence, dt_occurrence_nb = occurrences.occurence(dt_messenger, list_mots, col_message='message_clean_lower_ponctuation')   





#==============================================================================================