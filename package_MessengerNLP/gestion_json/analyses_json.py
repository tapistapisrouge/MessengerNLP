# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 13:02:08 2021

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
from datetime import datetime # pour traduire timestamp en date, très très chiant

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

# =============================================================================================

def df_videos(list_files_json):

    # créer le dataframe avec participants, années et stats générales 
    df_videos = pd.DataFrame(columns=['participants',
                                      'timestamp_ms',
                                      'video_uri',
                                      'video_timestamp'])


    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        print(file)
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages') 
        for message in data_messages:
            if 'videos' in message:
                # sender_name
                participant = message.get('sender_name')
                # récupération timestamp
                timestamp_ms = message.get('timestamp_ms')
                # recup infos video
                list_videos = message.get('videos')
                if not(list_videos):
                    print('piste video vide')
                    print(timestamp_ms)
                else:
                    for video in list_videos:
                        video_uri = video.get('uri')
                        video_timestamp= video.get('creation_timestamp')
                        # ajout dans le dataframe
                        df_videos = df_videos.append({'participants':participant,
                                                      'timestamp_ms':timestamp_ms,
                                                      'video_uri':video_uri,
                                                      'video_timestamp':video_timestamp }, 
                                                     ignore_index=True)                  


    # timstamp to datetime
    df_videos['datetime'] = datetime.now()
    for i in range(len(df_videos['timestamp_ms'])):
        timestamp_temp = df_videos.loc[i,'timestamp_ms']
        df_videos.loc[i,'datetime'] = datetime.fromtimestamp(timestamp_temp/1000.0)
        
    #iden timestamp video creation
    df_videos['datetime_video'] = datetime.now()
    for i in range(len(df_videos['video_timestamp'])):
        timestamp_temp = df_videos.loc[i,'video_timestamp']
        df_videos.loc[i,'datetime_video'] = datetime.fromtimestamp(timestamp_temp)    
    
    return(df_videos)


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
    
    # reaction infos
    df_videos_info = df_videos(list_files_json)
    
    # filtre joel
    # df_videos_joel = df_videos_info[df_videos_info['participants']=='Joël Le Galudec']
    

# =============================================================================================

def df_call(list_files_json):

    # créer le dataframe avec participants, années et stats générales 
    df_call = pd.DataFrame(columns=['participants',
                                    'timestamp_ms',
                                    'content',
                                    'call_duration'])


    # fonction de substitution de caractère 
    fix_mojibake_escapes = partial(
        re.compile(rb'\\u00([\da-f]{2})').sub,
        lambda m: bytes.fromhex(m.group(1).decode()))

    for file in list_files_json: 
        # file = list_files_json[1]
        print(file)
        with open(file, 'rb') as binary_data:
            repaired = fix_mojibake_escapes(binary_data.read())
        data_dict = json.loads(repaired.decode('utf8'))
     
        # liste des messages
        data_messages = data_dict.get('messages') 
        for message in data_messages:
            if message.get('type')=='Call':
                # sender_name
                participant = message.get('sender_name')
                # récupération timestamp
                timestamp_ms = message.get('timestamp_ms')
                # recup infos video
                content = message.get('content')
                # info call_duration
                if 'call_duration' in message:
                    call_duration = message.get('call_duration')
                else:
                    call_duration = None
                df_call = df_call.append({'participants':participant,
                                          'timestamp_ms':timestamp_ms,
                                          'content':content,
                                          'call_duration':call_duration}, 
                                         ignore_index=True)                  


    # timstamp to datetime
    df_call['datetime'] = datetime.now()
    for i in range(len(df_call['timestamp_ms'])):
        timestamp_temp = df_call.loc[i,'timestamp_ms']
        df_call.loc[i,'datetime'] = datetime.fromtimestamp(timestamp_temp/1000.0)
    
    return(df_call)


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
    
    # reaction infos
    df_call = df_call(list_files_json)











    