# -*- coding: utf-8 -*-
"""
Created on Tue Sep  7 17:18:53 2021

@author: AmauryP

Fonctions utiles au traitement de base des fichiers json messenger :
    - Lister l'ensemble des fichiers json d'une conversation
    - Différentes fonctions de nettoyage des messages (enlever caractères spéciaux, convertir les accents, enlever la ponctuation, etc)
    - Lister les participants d'une conversation
    - Transformation des json en un csv exploitable
"""

#==============================================================================================
# Modules/Packages à importer =================================================================
#==============================================================================================

import glob # pour lister les fichiers json d'un dossier en particulier
import json # pour gérer les fichiers json
import re
import pandas as pd # pour gérer des dataframes
import pathlib # pour gérer les chemins
from functools import partial
import numpy as np
from datetime import datetime # pour traduire timestamp en date, très très chiant

if __name__ == '__main__': 
    # packages perso
    import base_json

#==============================================================================================
# Création du dataframe =======================================================================
#==============================================================================================

if __name__ == '__main__': 
    dossier_convers = input("Entrer le nom du dossier contenant les fichiers json voulus : ")
    print('Vous avez écrit : ', dossier_convers)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'input')
    path_convers = pathlib.Path(path_convers, dossier_convers)
    print(path_convers)
    
    # on liste les fichiers json
    list_files_json = base_json.list_files_json(path_convers)
    


#==============================================================================================
# Fonctions ===================================================================================
#==============================================================================================

def nettoyer_carac_speciaux(chaine_a_nettoyer):
    # à modifier plus simple de faire :
    motif = re.compile('[^a-zA-z0-9œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏî ÎüÜùÙûÛçÇ%€«»";:\(\)\'\-\!\?\.]')
    message_clean = motif.sub("", chaine_a_nettoyer)
    motif_bis = re.compile('[\\\\_\[\]\^]')
    message_clean = motif_bis.sub("", message_clean)
    return(message_clean)

if __name__ == '__main__': 
    message_test = 'Ç\'est bien.;; salut où es-tu l\'ami "" «lol»(10% ?) - _ [^^\\^^] œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ%€ \uf0e8'
    print(message_test)
    message_clean = nettoyer_carac_speciaux(message_test)
    print(message_clean)


def nettoyer_ponctuation(chaine_a_nettoyer):
    # caracteres très spéciaux
    # motif = re.compile("[«»;()\"\:\!\?\.]")
    # motif = re.compile("[^a-zA-z0-9 œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ']")
    # motif = re.compile("[^a-zA-z0-9]")
    motif = re.compile('[^a-zA-Z0-9œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ\'\- ]')
    message_clean = motif.sub("",chaine_a_nettoyer)
    return(message_clean)

if __name__ == '__main__': 
    message_test = 'Ç\'est bien.;; salut où es-tu l\'ami "" «lol»(10% ?) - _ [^^\\^^] œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ%€ \uf0e8'
    print(message_test)
    message_clean = nettoyer_ponctuation(message_test)
    print(message_clean)


def json_to_csv(list_files_json, path_csv_output, other_participant='autre'):
    
    ############################################
    ######## Créer le dataframe initial ########
    ############################################
    
    # creation d'un dataframe qui aura nom des gens, messages et date/heure du message, vide pour l'instant
    messenger_dataframe=pd.DataFrame(columns=['participants',
                                            'message_brut',
                                            'message_clean',
                                            'message_clean_lower_ponctuation',
                                            'type',
                                            'timestamp_ms'])

    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))
    
    # extraction des données en dataframe
    for file in list_files_json:     
        print(file)
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
         
        # on extrait du dictionnaire général la liste des messages : {'messages':[liste de 10 000 messages]}
        # c'est une liste de dictionnaire, un message est un dictionnaire
        # les keys intéressantes : sender_name, timestamp_ms, content, type, reactions, photos, gifs, videos, share, files, audio_files, users, sticker
        
        #recupération des données pour chaque message        
        list_messages=data_dict.get('messages')
        for message in list_messages:
            # type(message) => dictionnaire, exemple {'sender_name':'Diem Ngo','content':'coucou','timestamp_ms'='1564465468486'}
            # recup du pseudo
            sender_name = str(message.get('sender_name'))
            sender_name = nettoyer_carac_speciaux(sender_name)
            if sender_name == 'Utilisateur Facebook':
                sender_name = other_participant
            # recup du timestamp
            timestamp_ms=int(message.get('timestamp_ms'))
            if 'content' in message:
                content = message.get('content')
                type_message = message.get('type')
            elif 'photos' in message:
                content=""
                type_message="photos"
            elif 'videos' in message:
                content=""
                type_message='videos'
            elif 'gifs' in message:
                content=""
                type_message='gifs'
            elif 'audio_files' in message:
                content=""
                type_message='audio_files'
            elif 'files' in message:
                content=""
                type_message='files'          
            elif 'sticker' in message:
                content=""
                type_message='sticker' 
            else:
                content=""
                type_message='anormal'    
            
            # content = texte brut
            # content_clean = sans les caractères spéciaux de merde
            content_clean = nettoyer_carac_speciaux(content)
            # content_clean_lower = on enlève la ponctuation et les majuscule
            content_clean_lower_ponctuation = nettoyer_ponctuation(content_clean).lower()
            

            # on met les info dans la dataframe junkies
            messenger_dataframe = messenger_dataframe.append({'participants':sender_name,
                                                          'message_brut':content,
                                                          'message_clean':content_clean,
                                                          'message_clean_lower_ponctuation':content_clean_lower_ponctuation,
                                                          'type':type_message,
                                                          'timestamp_ms':timestamp_ms}, 
                                                         ignore_index=True)

    
    # dtypes gestion
    messenger_dataframe['participants']= messenger_dataframe['participants'].apply(str)
    messenger_dataframe['message_brut']= messenger_dataframe['message_brut'].apply(str)
    messenger_dataframe['message_clean']= messenger_dataframe['message_clean'].apply(str)
    messenger_dataframe['message_clean_lower_ponctuation']= messenger_dataframe['message_clean_lower_ponctuation'].apply(str)
    messenger_dataframe['type']= messenger_dataframe['type'].apply(str)
    messenger_dataframe['timestamp_ms']= messenger_dataframe['timestamp_ms'].apply(np.int64)

    # enregistrer rapidement avant manipulation
    #path = 'H:/Projet_Code/junkies_classification'
    #path_output=pathlib.Path(path, 'output')
    #fichier_csv="convers_entiere.csv"
    #path_csv_output=pathlib.Path(path_output, fichier_csv)
    #junkies_dataframe.to_csv(path_csv_output, encoding='utf-8-sig', sep=';', decimal='.', index=False)
    #encoding='utf-8' ==> ne marche pas
    # encoding='utf-8-sig' ==> nickel
    
    print('debut datetime')
    # boucle transfo des timestamp en datetime
    messenger_dataframe['datetime'] = datetime.now()
    for i in range(len(messenger_dataframe['timestamp_ms'])):
        timestamp_temp = messenger_dataframe.loc[i,'timestamp_ms']
        messenger_dataframe.loc[i,'datetime'] = datetime.fromtimestamp(timestamp_temp/1000.0)
    
    
    # recup des données du datetime, utile pour filtrer le dataframe selon année, mois, autre
    messenger_dataframe['annee'] = messenger_dataframe['datetime'].dt.year
    #test_data['Annee'] = test_data['Datetime'].dt.strftime('%Y')
    #test_data['Annee'] = pd.DatetimeIndex(test_data['Datetime']).year
    messenger_dataframe['mois']=messenger_dataframe['datetime'].dt.month
    messenger_dataframe['jour']=messenger_dataframe['datetime'].dt.day
    messenger_dataframe['heure']=messenger_dataframe['datetime'].dt.hour
    messenger_dataframe['minute']=messenger_dataframe['datetime'].dt.minute
    messenger_dataframe['seconde']=messenger_dataframe['datetime'].dt.second
    messenger_dataframe['weekday_int']=messenger_dataframe['datetime'].dt.dayofweek
    dayOfWeek={0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}
    messenger_dataframe['weekday_name'] = messenger_dataframe['datetime'].dt.dayofweek.map(dayOfWeek)
    
    print('longueur message')
    # ajouter colonne longeur message
    for i in range(len(messenger_dataframe['message_brut'])):
        messenger_dataframe.loc[i,'length_brut'] = len(str(messenger_dataframe.loc[i,'message_brut']))
    messenger_dataframe['length_brut'] = messenger_dataframe['length_brut'].apply(int)
    
    
    # trier selon timestamp
    messenger_dataframe.sort_values(by = 'timestamp_ms', ascending = True, inplace = True)

    # enregistrer le dataframe final    
    #path = 'H:/Projet_Code/junkies_classification'
    #path_output=pathlib.Path(path, 'output')
    #fichier_csv="convers_entiere_correction.csv"
    #path_csv_output=pathlib.Path(path_output, fichier_csv)
    messenger_dataframe.to_csv(path_csv_output, encoding='utf-8-sig', sep=';', decimal='.', index=False)
    #encoding='utf-8' ==> ne marche pas

    return(messenger_dataframe)


if __name__ == '__main__': 
    
    path_csv_output = pathlib.Path(__file__)
    path_csv_output = path_csv_output.parent.parent.parent
    path_csv_output = pathlib.Path(path_csv_output, 'output')
    path_csv_output = pathlib.Path(path_csv_output, dossier_convers)
    print(path_csv_output)
    
    # on crée le dossier s'il n'existe pas
    path_csv_output.mkdir(parents=True, exist_ok=True)
    
    # on termine le nom du csv
    path_csv_output = pathlib.Path(path_csv_output, "df_messenger.csv")
    
    # endroit où on enregistre le dataframe
    messenger_dataframe = json_to_csv(list_files_json, path_csv_output)
    
    











