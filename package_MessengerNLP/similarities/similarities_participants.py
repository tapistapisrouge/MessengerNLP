# -*- coding: utf-8 -*-
"""
Created on Sun Nov 28 16:54:50 2021

@author: amaur
"""


#==============================================================================================
# Modules/Packages à importer =================================================================
#==============================================================================================

import pandas as pd
import datetime # pour traduire timestamp en date
import pathlib # pour gérer les chemins
import numpy as np

# packages perso
import sys
import os

# similarités analyse
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords

# sys.path.insert(0, os.path.join(__file__, '..', '..','gestion_json'))
# OU
# =============================================================================
# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# print(SCRIPT_DIR)
# sys.path.append(os.path.dirname(SCRIPT_DIR))
# from gestion_json import base_json
# =============================================================================

# si besoin en test
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/similarities/similarities_participants.py'

#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================

def csv_to_txt_participants(df_messenger, path_txt, nom_col_message):
    # on crée le dossier qui aura les fichier textes (output/nom_convers/txt_participants)
    path_txt_final = pathlib.Path(path_txt, 'txt_participants')
    path_txt_final.mkdir(parents=False, exist_ok=True)
    
    # on crée la liste des participants
    list_participants = list(df_messenger['participants'].unique())
    
    # on nettoie le csv initial=====================
    #filtrer sur type="Generic"
    df_messenger = df_messenger[df_messenger['type']=='Generic'] 
    # enlever message na
    df_messenger = df_messenger.dropna()
    # enlever message vide
    df_messenger = df_messenger[df_messenger[nom_col_message] != '']
    # mettre message en string
    df_messenger = df_messenger.astype({nom_col_message: object})
    # enlever ligne commençant par http
    mask = df_messenger[nom_col_message].str.match("[^http]")
    df_messenger = df_messenger[mask]
    
    # on fait le job participant par participant :
    for participant in list_participants:
        print('script travaillant actuellement sur le cas de : ', participant)
        df_participant = df_messenger[df_messenger['participants']==participant]
        # mettre en liste
        list_of_messages = df_participant[nom_col_message].tolist()
        # list to str
        convers_text=" ".join(list_of_messages)
        # nettoyage double espace :
        convers_text = ' '.join(convers_text.split())
        # enregistrement en txt
        participant_without_space = ''.join(participant.split())
        participant_clean = participant_without_space.lower()
        nom_fichier = participant_clean + '.txt'
        path_convers_txt = pathlib.Path(path_txt_final, nom_fichier)
        # ecriture du fichier (r = read, a = write mode append, w = creates a new file or truncates an existing file, then opens it for writing; the file pointer position at the beginning of the file.)
        with open(path_convers_txt, "w") as fichier:
            fichier.write(convers_text)


if __name__ == '__main__':
    # chemin convers csv
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers csv voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'output')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    path_convers = pathlib.Path(path_convers, 'df_messenger.csv')
    print(path_convers)
    #chemin dossier où enregistrer les fichiers txt
    path_txt = pathlib.Path(__file__)
    path_txt = path_txt.parent.parent.parent
    path_txt = pathlib.Path(path_txt, 'output')
    path_txt = pathlib.Path(path_txt, dossier_convers)    
    
    # on ouvre le dataframe
    df_messenger = pd.read_csv(path_convers, sep=';', decimal='.')
    
    # nom colonne des messages à prendre 
    nom_col_message = 'message_clean_lower_ponctuation'

    # lancement fonction
    csv_to_txt_participants(df_messenger, path_txt, nom_col_message)


# =============================================================================================

def similarite_participants(path_txt):
    list_txt=[]
    for file in os.listdir(path_txt):
        if file.endswith(".txt"):
            list_txt.append(file)
    
    list_participants = []
    corpus = []
    # dict_txt_participants = dict()
    for file in list_txt:
        path_txt_participant = pathlib.Path(path_txt, file)
        # nom du participant
        participant = file[:-4]
        list_participants.append(participant)
        # lecture du fichier text
        with open(path_txt_participant, "r") as fichier:
            text = fichier.read()
        corpus.append(text)
        # ajout au dictionnaire
        # dict_txt_participants[participant] = text
    
    # étude similarité
    list_stopwords_french = stopwords.words('french')
    vect = TfidfVectorizer(min_df=1, stop_words=list_stopwords_french) 
    tfidf = vect.fit_transform(corpus)                                                                                                                                                                                                                       
    pairwise_similarity = tfidf * tfidf.T *100
    # transfo données en np.array
    data_numpy = pairwise_similarity.toarray()
    # mise en forme dans un dataframe final
    df = pd.DataFrame(data = data_numpy, index=list_participants, columns=list_participants)
    
    return(df)
        

if __name__ == '__main__':
    # chemin convers csv
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers csv voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_txt = pathlib.Path(__file__)
    path_txt = path_txt.parent.parent.parent
    path_txt = pathlib.Path(path_txt, 'output')
    path_txt = pathlib.Path(path_txt, dossier_convers)
    path_txt = pathlib.Path(path_txt, 'txt_participants')
    print(path_txt)
    
    result_similarities = similarite_participants(path_txt)
    
    
