# -*- coding: utf-8 -*-
"""
@author: amaur
Module de calcul d'occurrence de mot depuis le fichier csv d'une convers Messenger
Fonctions :
    - most_frequent : liste des mots les plus frequent du fichier txt (global ou par personne selon le txt)
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
from collections import defaultdict, Counter

# packages perso
import sys
import os

print(__file__)

# si besoin en test
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/gestion_txt/occurrences_txt.py'


#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================

def most_frequent(chemin_txt):
    """
    A venir
    """
    with open(chemin_txt, "r") as fichier:
        convers_txt = fichier.read()
    print(type(convers_txt))
    
    # occurence max
    most_frequence = Counter(convers_txt.split()).most_common(1000)
    df_occurrence_freq = pd.DataFrame(most_frequence, columns=['mot', 'occurrence'])
    
    return(df_occurrence_freq)
    
    
if __name__ == '__main__':   
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers txt voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'txt')
    path_convers = pathlib.Path(path_convers, 'rémicourbou.txt')
    print(path_convers)
    
    # on ouvre le dataframe
    df_most_freq = most_frequent(path_convers) 
    
    
    
    
    
    