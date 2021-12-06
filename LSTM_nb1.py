# -*- coding: utf-8 -*-
"""
Created on Sun Nov 21 20:18:28 2021

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

from collections import defaultdict, Counter
import markovify

from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout
from keras.layers import LSTM
from keras.callbacks import ModelCheckpoint
from keras.utils import np_utils

import numpy as np

# packages perso
# rien ici

#==============================================================================================
# Si besoin de tester ligne par ligne =========================================================
#==============================================================================================
__file__ = 'C:/Users/amaur/Documents/GitHub/MessengerNLP/LSTM_nb1.py'


#==============================================================================================
# Ouverture dataframe =========================================================================
#==============================================================================================
nom_fichier = 'df_messenger_reduit.txt'

dossier_convers = input("Entrer le nom du dossier contenant le fichier txt voulu : ")
print('Vous avez écrit : ', dossier_convers)
path_convers = pathlib.Path(__file__)
path_convers = path_convers.parent
path_convers = pathlib.Path(path_convers, 'output')
path_convers = pathlib.Path(path_convers, dossier_convers)
path_convers_txt = pathlib.Path(path_convers, nom_fichier)
print(path_convers_txt)



#==============================================================================================
# ouverture du fichier text et occurrence =====================================================
#==============================================================================================

# ouverture du fichier
# enregistrement en txt
with open(path_convers_txt, "r") as fichier:
    convers_junkies = fichier.read()
print(type(convers_junkies))

# lower
convers_junkies = convers_junkies.lower()

# =============================================================================
# message_test = 'Ç\'est bien.;; salut où es-tu l\'ami "" «lol»(10% ?) - _ œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ%€ \uf0e8'
# message_test_lower = message_test.lower()
# =============================================================================

# occurence max
most_occurrence_junkies = Counter(convers_junkies.split()).most_common(1000)
df_occurrence = pd.DataFrame(most_occurrence_junkies, columns=['mot', 'occurrence'])


#==============================================================================================
# LSTM =====================================================
#==============================================================================================

# create mapping of unique chars to integers
chars = sorted(list(set(convers_junkies)))
print(len(chars))
print(chars)
char_to_int = dict((c, i) for i, c in enumerate(chars))

n_chars = len(convers_junkies)
n_vocab = len(chars)
print("Total Characters: ", n_chars)
print("Total Vocab: ", n_vocab)

# prepare the dataset of input to output pairs encoded as integers
seq_length = 100
dataX = []
dataY = []
for i in range(0, n_chars - seq_length, 1):
	seq_in = convers_junkies[i:i + seq_length]
	seq_out = convers_junkies[i + seq_length]
	dataX.append([char_to_int[char] for char in seq_in])
	dataY.append(char_to_int[seq_out])
n_patterns = len(dataX)
print("Total Patterns: ", n_patterns)


# reshape X to be [samples, time steps, features]
X = np.reshape(dataX, (n_patterns, seq_length, 1))
# normalize
X = X / float(n_vocab)
# one hot encode the output variable
y = np_utils.to_categorical(dataY)
print(y.shape[1])

print(X.shape[1])
print(X.shape[2])

x_shape1 = X.shape[1]
x_shape2 = X.shape[2]

# define the LSTM model
model = Sequential()
model.add(LSTM(256, input_shape=(x_shape1, x_shape2)))
model.add(Dropout(0.2))
model.add(Dense(y.shape[1], activation='softmax'))
model.compile(loss='categorical_crossentropy', optimizer='adam')




