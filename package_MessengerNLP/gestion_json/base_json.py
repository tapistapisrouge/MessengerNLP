# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 12:11:47 2021

@author: amaur
"""

#==============================================================================================
# Modules/Packages à importer ===============================================================
#==============================================================================================

import glob # pour lister les fichiers json d'un dossier en particulier
import pathlib # pour gérer les chemins
from functools import partial
import re
import json
import datetime # pour traduire timestamp en date

if __name__ == '__main__':
    import operator


#==============================================================================================
# Si besoin de tester ligne par ligne ============================================
#==============================================================================================
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/gestion_json/base_json.py'

#==============================================================================================
# Création de la liste des fichiers json à traiter ============================================
#==============================================================================================

def list_files_json(chemin):
    
    # mettre le chemin en str
    chemin = str(chemin)
    chemin = chemin.replace('\\','/')

    # on crée le pattern à lister   
    chemin_final=chemin+'/*.json'
    # on fait une liste des chemins absolus de chaque fichier
    path_to_file_list = glob.glob(chemin_final) 
    
    # petit remplacement pour avoir le chemin sous la bonne forme ;)
    file_list = [i.replace('\\', '/') for i in path_to_file_list]
    
    # on retourner la liste voulue
    return(file_list)



if __name__ == '__main__':
    
    conversation_choix = input("Entrer le nom du dossier contenant la convers voulue : ")
    print('Vous avez écrit : ', conversation_choix)
    path_convers = pathlib.Path(__file__)
    path_convers = path_convers.parent.parent.parent
    path_convers = pathlib.Path(path_convers, 'input')
    path_convers = pathlib.Path(path_convers, conversation_choix)
    type(path_convers)
    path_convers
    print(path_convers)

    # lancer la fonction list_files_json() 
    list_files_json = list_files_json(path_convers)
    print(list_files_json)
    
#==============================================================================================
# Gestion des participants ====================================================================
#==============================================================================================

# fonction utile sur des convers avec des personnes parlant peu ==> nom du participants pas forcément écris sur tous les json donc 
# il faut lire l'ensemble des jsons
# Pour info, json = gros dictionnaire
# Chaque json a une clé "participants" dont la value est une liste des participants

def list_participants_totale(list_files_json):
    
    #creation de la list vide à remplir
    list_participants=[]
    
    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json:     
        # file = list_files_json[0]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))

        
        # on prend la liste des particpants (liste de dictionnaires)
        data_participants = data_dict.get('participants')
        for dico in data_participants:
            participant_temp = dico['name']
            if participant_temp not in list_participants:
                list_participants.append(participant_temp)

        #print(list_participants)        
        return(list_participants)


if __name__ == '__main__':
    
    list_participants=list_participants_totale(list_files_json)
    print(list_participants)




#==============================================================================================
# Lister tous ce qui est possible de lister !!!================================================
#==============================================================================================

def list_cles_messages(list_files_json):
    #lister les différentes clés des messages
    list_cles=[]

    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json:     
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages')      
        # on check chaque clés
        for message in data_messages:
            for key in message.keys():
                if key not in list_cles:
                    list_cles.append(key)
    
    return(list_cles)


if __name__ == '__main__':   
    list_cles = list_cles_messages(list_files_json)
    print(list_cles)


#===============================================================

def list_reactions(list_files_json):
    
    #creation de la list vide à remplir
    list_reactions = []
    
    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json:     
        # file = list_files_json[1]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))

        # on prend la liste des particpants (liste de dictionnaires)
        data_messages = data_dict.get('messages')
        for message in data_messages:
            if ('reactions' in message):
                reactions = message.get('reactions')
                for reaction in reactions:
                    reaction_temp = reaction.get('reaction')
                    # print(reaction_temp)
                    if reaction_temp not in list_reactions:
                        list_reactions.append(reaction_temp)                    
                 
    # print(list_reactions)        
    return(list_reactions)

if __name__ == '__main__':   
    list_reactions = list_reactions(list_files_json)
    print(list_reactions)


#===============================================================

def dict_caracteres(list_files_json):
    
    #creation de la list vide à remplir
    dict_caracteres = {}
    
    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json:     
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))

        # on prend la liste des particpants (liste de dictionnaires)
        data_messages = data_dict.get('messages')
        for message in data_messages:
            # message = data_messages[15]
            if ('content' in message) and (message.get('content') is not None):
                message_temp = message.get('content')
                for char in message_temp:
                    dict_caracteres[char] = dict_caracteres.get(char, 0) + 1
                                  
        # print(list_reactions)        
        return(dict_caracteres)


if __name__ == '__main__':   
    dict_caracteres = dict_caracteres(list_files_json)
    print(dict_caracteres)
    dict_caracteres_sorted = sorted(dict_caracteres.items(), key=operator.itemgetter(1))

#===============================================================

def list_annees(list_files_json):
    
    #creation de la list vide à remplir
    list_annees = []
    
    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))

        # on prend la liste des particpants (liste de dictionnaires)
        data_messages = data_dict.get('messages')
        for message in data_messages:
            # message = data_messages[7000]
            if 'timestamp_ms' in message:
                timestamp_temp = message.get('timestamp_ms')
                date_temp = datetime.datetime.fromtimestamp(timestamp_temp/1000.0)
                annee_temp = date_temp.year
                if annee_temp not in list_annees:
                    list_annees.append(annee_temp)
                                  
    # print(list_annees) 
    list_annees = sorted(list_annees)       
    return(list_annees)


if __name__ == '__main__':   
    list_annees = list_annees(list_files_json)
    print(list_annees)

 
#===============================================================

def list_cles_messages(list_files_json):

    #lister les différentes clés des messages
    list_cles=[]
    
    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))

        # on prend la liste des particpants (liste de dictionnaires)
        data_messages = data_dict.get('messages')
        
        # on récupère les clés
        for message in data_messages:
            for key in message.keys():
                if key not in list_cles:
                    list_cles.append(key)
   
    # enlever les cles inutiles
    list_cles.remove('timestamp_ms')
    list_cles.remove('is_unsent')
    list_cles.remove('type')
    
    return(list_cles)


if __name__ == '__main__':   
    list_cles_messages = list_cles_messages(list_files_json)
    print(list_cles_messages)


      
    