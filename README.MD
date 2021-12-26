#########################################################
############## Tuto rapide ##############################
#########################################################

1. Télécharger vos conversation messenger depuis facebook
Allez dans Paramètres et confidentialité -> Paramètres -> Vos informations Facebook -> Télécharger vos informations
Sélectionnez le format json et cochez messages
Il faudra attendre quelques heures avant d'avoir le zip de vos données

2. Retrouver vos convers facebook
Souvent /fichier_facebook/inbox/nom_de_conversation
nomenclature = message_X.json (X allant de 1 à infini)

2. Créer un dossier dans /input avec un nom au choix
Exemple : /input/convers_famille

3. Coller les fichiers json dans le dossier /input/nom_convers

4. Lancer les scripts dans l'ordre :
- 1_json_to_dataframe.py 
  sans surprise pour transformer tous les json de votre dossier en fichier csv, trouvable dans /output/nom_convers
  le script vous demandera : "Entrer le nom du dossier contenant les fichiers json voulus :"
- 2_dataframe_to_txt.py ==> crée un .txt avec la conversation (caractère en lower, caractères gardés : a-zA-Z0-9œŒæÆàÀâÂäÄéÉèÈëËêÊöÖôÔïÏîÎüÜùÙûÛçÇ\'\- ]')
 + 1 .txt par personne de la convers
 fichier trouvable dans \output\nom_convers\txt
- 3_stats_json_csv.py
 crée différentes stats et graph dans /output/nom_convers/stats_csv et /output/nom_convers/stats_json
- 4_occurrences_graph.py : modifiez ligne 48 la liste en mettant els mots voulus (nombre de mot non limité, vous pouvez mettre 1 à infini de mot)
- 5_LSTM_nb1.py, en travaux

