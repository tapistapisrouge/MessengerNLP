# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 12:11:47 2021

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
import datetime # pour traduire timestamp en date

# packages perso
import sys
import os
sys.path.insert(0, os.path.join(__file__, '..'))
import base_json

# si besoin en test
# __file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/package_MessengerNLP/gestion_json/stats_json.py'


#==============================================================================================
# Fonctions  ==================================================================================
#==============================================================================================


def json_stats_globales(list_files_json, list_participants, list_annees, list_cles, 
                        facebook_utilisateur='autre', other_participant=True):
    
    # ajout de "autre" dans la liste des participants (pb avec Utilisateur Facebook si compte supprimé)
    if other_participant==True:
        list_participants.append('autre')
    
    # créer le dataframe avec participants, années et stats générales 
    dt_stats = pd.DataFrame(columns = ['participants','annees'])
    for participant in list_participants:
        for annee in list_annees:
            dt_stats = dt_stats.append({'participants': participant, 'annees': annee}, ignore_index=True)
    for cle in list_cles:
        dt_stats[cle] = 0


    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[0]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages') 
        for message in data_messages:
            # nom de la personne
            participant = message.get('sender_name')
            if participant not in list_participants:
                participant = facebook_utilisateur
            # récupération l'annee
            timestamp_temp = message.get('timestamp_ms')
            date_temp = datetime.datetime.fromtimestamp(timestamp_temp/1000.0)
            annee_temp = int(date_temp.year)
            # list des clés du message 
            list_cles_temp = list(message.keys())
            # stockage info
            for cle in list_cles_temp:
                if cle in list_cles:
                    # print(cle)
                    index_list = dt_stats[(dt_stats['participants']==participant)&(dt_stats['annees']==annee_temp)].index.tolist()
                    ligne=index_list[0]
                    # print(ligne)
                    dt_stats.loc[ligne, cle] += 1
    
    # construction de 4 dataframes, selon regroupement année et pourcentage
    
    # changer nom de 2 colonnes, sender_name = intervention et content = message
    dt_stats.rename(columns={'sender_name': 'interventions', 
                          'content': 'messages'}, inplace=True)
     
    # en pourcentage
    list_colonnes_1 = list(dt_stats.columns)
    list_colonnes = list(dt_stats.columns)
    list_colonnes.remove('participants')
    list_colonnes.remove('interventions')
    list_colonnes.remove('annees')
    
    list_colonnes_pourcentage = ['participants', 'annees']
    for colonne in list_colonnes:
        nom_col = colonne + "_%"
        list_colonnes_pourcentage.append(nom_col)
        dt_stats[nom_col] = dt_stats[colonne]/dt_stats['interventions']*100
    
    # separation en 2 dataframe
    dt_stats_globales = dt_stats.loc[:,list_colonnes_1]    
    dt_stats_globales_pourcentage = dt_stats.loc[:,list_colonnes_pourcentage] 
    
    # regrouper par colonne années
    dt_stats_group = dt_stats_globales.groupby(['participants']).sum()
    dt_stats_group.reset_index(inplace=True) 
    for colonne in list_colonnes:
        nom_col = colonne + "_%"
        dt_stats_group[nom_col] = dt_stats_group[colonne]/dt_stats_group['interventions']*100
    
    # separation en 2 dataframe
    list_colonnes_1.remove('annees')
    list_colonnes_pourcentage.remove('annees')
    dt_stats_globales_group = dt_stats_group.loc[:,list_colonnes_1]    
    dt_stats_globales_pourcentage_group = dt_stats_group.loc[:,list_colonnes_pourcentage]    

    return(dt_stats_globales, dt_stats_globales_pourcentage, dt_stats_globales_group, dt_stats_globales_pourcentage_group)


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
    
    # reaction infos
    dt_stats_globales, dt_stats_globales_pourcentage, dt_stats_globales_group, dt_stats_globales_pourcentage_group = json_stats_globales(list_files_json, list_participants, list_annees, list_cles, other_participant=True)
    


# =============================================================================================

def reaction_infos(list_files_json, list_participants, list_annees, list_reactions,
                   facebook_utilisateur='autre', other_participant=True):
    
    # ajout de "autre" dans les listes en cas où
    if other_participant==True:
        list_participants.append('autre')
    # normalement aucun risque avec les reactions
    # list_reactions.append('autre')
    
    # créer le dataframe avec participants, années et stats générales 
    dt_reactions = pd.DataFrame(columns = ['participants','annees','reactions'])
    for participant in list_participants:
        for annee in list_annees:
            for reaction in list_reactions:
                dt_reactions = dt_reactions.append({'participants': participant, 'annees': annee, 'reactions': reaction}, ignore_index=True)
    for participant in list_participants:
        dt_reactions[participant] = 0


    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages') 
        for message in data_messages:
            if 'reactions' in message:
                # récupération l'annee
                timestamp_temp = message.get('timestamp_ms')
                date_temp = datetime.datetime.fromtimestamp(timestamp_temp/1000.0)
                annee_temp = date_temp.year
                # celui qui recoit les reactions
                receveur_reaction = message.get('sender_name')
                if receveur_reaction not in list_participants:
                    participant = facebook_utilisateur
                # liste des reactions
                list_temp = message.get('reactions')
                if not(list_temp):
                    print('liste vide')
                else:
                    for reaction in list_temp:
                        donneur_reaction = reaction.get('actor')
                        if donneur_reaction not in list_participants:
                            donneur_reaction = facebook_utilisateur
                        reaction_temp = reaction.get('reaction')
                        # on stock l'info
                        index_list = dt_reactions[(dt_reactions['participants']==receveur_reaction)&(dt_reactions['annees']==annee_temp)&(dt_reactions['reactions']==reaction_temp)].index.tolist()
                        ligne = index_list[0]
                        dt_reactions.loc[ligne,donneur_reaction]+=1                    
   
    
    # regrouper par colonne années
    dt_reactions_group = dt_reactions.groupby(['participants', 'reactions']).sum()
    dt_reactions_group.reset_index(inplace=True)  
    # si besoin de passer les index en colonnes
    # cols = dt_reactions_group.columns.tolist()
    # cols = cols[-1:] + cols[:-1]
    # dt_reactions_group = dt_reactions_group[cols] 
    
    # avoir les pourcentages
    
    return(dt_reactions, dt_reactions_group)


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
    print(list_files_json)
    
    # on liste les participants
    list_participants = base_json.list_participants_totale(list_files_json)
    print(list_participants)
    
    # liste des années dispo dans la convers
    list_annees = base_json.list_annees(list_files_json)
    print(list_annees)
    
    # on liste les reactions
    list_reactions = base_json.list_reactions(list_files_json)
    
    # reaction infos
    dt_reactions, dt_reactions_group = reaction_infos(list_files_json, list_participants, list_annees, list_reactions, other_participant=True)
    
    
    
    
# =============================================================================================

def kick_infos(list_files_json, list_participants, list_annees,
               facebook_utilisateur='autre', other_participant=True):
    
    # ajout de "autre" dans les listes en cas où
    if other_participant == True:
        list_participants.append('autre')
    # normalement aucun risque avec les reactions
    # list_reactions.append('autre')
    
    # créer le dataframe avec participants, années et stats générales 
    dt_kick = pd.DataFrame(columns = ['participants'])
    for participant in list_participants:
        for annee in list_annees:
            dt_kick = dt_kick.append({'participants': participant, 'annees': annee}, ignore_index=True)
    for participant in list_participants:
        dt_kick[participant] = 0


    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages') 
        for message in data_messages:
            type_message = message.get('type')
            if type_message=='Unsubscribe':
                # récupération l'annee
                timestamp_temp = message.get('timestamp_ms')
                date_temp = datetime.datetime.fromtimestamp(timestamp_temp/1000.0)
                annee_temp = date_temp.year
                # on regarde qui a kické
                participant = message.get('sender_name')
                if participant not in list_participants:
                    participant = facebook_utilisateur
                # on regarde qui est kické
                try:
                    users = message.get('users')[0]
                    kicked_man = users.get('name')
                    if kicked_man not in list_participants:
                        kicked_man = facebook_utilisateur
                except IndexError:
                    print ("No man kicked")
                # on ajoute +1 au bon endroit
                index_list = dt_kick[(dt_kick['participants']==kicked_man)&(dt_kick['annees']==annee_temp)].index.tolist()
                ligne = index_list[0]
                dt_kick.loc[ligne,participant]+=1                       
   
    
    # regrouper par colonne années
    dt_kick_group = dt_kick.groupby(['participants']).sum()
    dt_kick_group['participants'] = dt_kick_group.index
    cols = dt_kick_group.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    dt_kick_group = dt_kick_group[cols]   
    
    return(dt_kick, dt_kick_group)


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
    print(list_files_json)
    
    # on liste les participants
    list_participants = base_json.list_participants_totale(list_files_json)
    print(list_participants)
    
    # liste des années dispo dans la convers
    list_annees = base_json.list_annees(list_files_json)
    print(list_annees)
    
    # reaction infos
    dt_kick, dt_kick_group = kick_infos(list_files_json, list_participants, list_annees, other_participant=True)
        