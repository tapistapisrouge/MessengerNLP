# -*- coding: utf-8 -*-
"""
Module df_to_txt
--> contient des fonction qui ont pour objectif de transformer les messages d'une conversation messenger
contenus dans un dataframe en fichier txt directement (pour être utilisé dans un modèle de generation de texte)

Fonctions actuelles :
    - df_muet qui retourne un dataframe donnant le nb de jour sans parler de chaque participant pour chaque année
    - df_nb_messages qui retrounent 2 dtaframes contenant le nb de message par participants par année et par année/mois
"""

#==============================================================================================
# Modules/Packages à importer =================================================================
#==============================================================================================


import re
import pandas as pd
import datetime # pour traduire timestamp en date

import pathlib # pour gérer les chemins



#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================

def df_to_txt(df_messenger, nom_col='message_clean', 
              filtre_annee=False, annee_debut=None, annee_fin=None,
              separateur=' '):
    """
    df_to_txt(df_messenger, nom_col='message_clean', 
              filtre_annee=False, annee_debut=None, annee_fin=None,
              separateur=' ')
    --> 6 parametres : 
        df_messenger = dataframe contenant la conversation messenger 
        nom_col = le nom de la colonne à prendre en compte pour extraire les messages, 'message_clean" par défaut
        filtre_annee = True/False, savoir si on prend tout ou seulement certaines années
        annee_debut et annee_fin = savoir quelles années prendre en compte
        separateur : choisir le séparateur des messages (espace par défaut)
    --> 1 return : convers_text, fichier texte, chaque 
    """
    
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
    convers_text = separateur.join(list_of_messages)
    
    # nettoyage double espace :
    convers_text = ' '.join(convers_text.split())
    
    return(convers_text)

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

    # muet infos
    convers_txt = df_to_txt(df_messenger, nom_col='message_clean_lower_ponctuation', 
                            filtre_annee=False, annee_debut=None, annee_fin=None,
                            separateur=' ')

# =============================================================================================


def df_to_txt_participants(df_messenger, nom_col='message_clean', 
              filtre_annee=False, annee_debut=None, annee_fin=None, separateur=' '):
    """
    df_to_txt_participants(df_messenger, list_participant, nom_col='message_clean', 
              filtre_annee=False, annee_debut=None, annee_fin=None, separateur=' ')
    --> 6 parametres : 
        df_messenger = dataframe contenant la conversation messenger 
        nom_col = le nom de la colonne à prendre en compte pour extraire les messages, 'message_clean" par défaut
        filtre_annee = True/False, savoir si on prend tout ou seulement certaines années
        annee_debut et annee_fin = savoir quelles années prendre en compte
        separateur : choisir le séparateur des messages (espace par défaut)
    --> 1 return : list_convers_text, liste de tous les fichiers texte
    """
    
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
    
    # participant par participant
    list_participants = df_messenger['participants'].unique()
    dict_convers_text = {}
    
    for participant in list_participants:
        df_messenger_participant = df_messenger[df_messenger['participants']==participant]    
        # mettre en liste
        list_of_messages = df_messenger_participant[nom_col].tolist()
        print('Nombre de messages : ', len(list_of_messages))

        #==============================================================================================
        # Création de textes ==========================================================================
        #==============================================================================================
        
        # list to str
        convers_text = separateur.join(list_of_messages)
        
        # nettoyage double espace :
        convers_text = ' '.join(convers_text.split())
        
        # ajout au dictionnaire
        dict_convers_text.update( {participant : convers_text} )
    
    return(dict_convers_text)


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

    # muet infos
    dict_convers_text = df_to_txt_participants(df_messenger, nom_col='message_clean_lower_ponctuation', filtre_annee=False, 
                                               annee_debut=None, annee_fin=None, separateur=' ')