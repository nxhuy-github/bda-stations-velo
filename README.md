## Membres du grooupe
* Abdenacer KERAGHEL    (DS)
* Huy NGUYEN XUAN       (DS)
* Amrine Amine MOSSAB   (TIW)
* Sarah TOUATIOUI       (TIW)
* Hung duy MAI          (TIW)


## Présentation
Nous disposons de données de stations vélos que nous récupérons depuis une api Web ainsi qu'un fichier CSV contenant l’historique.

Nous effectuons des traitements sur ces données afin de détecter des situations ou l'on souhaiterait envoyer des alertes. afficher des statistiques sur une station donnée.

Nous vondrons envoyer des messages d’alertes si:
- Il n'y a plus de vélos disponibles.
- Il n'y a plus de places disponibles.
- La disponibilité en vélos d'une stations sur les 3 dernières heures est inférieure à 5.

Nous voudrons aussi afficher un graphique de taux d'occupation pour une station donnée.

Enfin enrichir la capacité de prévision du système pour anticiper les alertes.



Remarques:
- Link to data : https://dl.univ-lyon1.fr/8vzqrlm88h
- Les timestamps fournit dans le CSV ne sont pas conformes au standard unix,
en effet ceux-ci correspondent au nombre de microsecondes*10 écoulées entre la date et le 01/01/0001.
Il faudra donc penser à convertir les dates présentes dans le fichier CSV.
- Le fichier sample peut être chargé dans un tableur pour voir à quoi ressemblent les données.
- Le schema est le suivant: ID;timestamp;hour;day_of_week;available_bike_stands;available_bikes

## Technologies
* KAFKA.
* STORM.
* HIVE.
* Zeppelin.


## Exécution
A lancer une fois uniquement sur la VM.
- Lancer zookeeper si ce n'est déjà fait

        /path/to/kafka/bin/zookeeper-server-start.sh /path/to/kafka config/zookeeper.properties    

- Lancer kafka si ce n'est déjà fait

        /path/to/kafka/bin/kafka-server-start.sh /path/to/kafka/config/server.properties


- Lancer nimbus, storm ui et supervisor. Dans storm/bin:
    
        ./storm nimbus
        ./storm ui
        ./storm supervisor

- Lancer zeppelin
        
        zeppelin/bin/zeppelin-daemon.sh start

## Scénario
### Producteur:

Le producteur se charge de récupérer les données de l'API web et les envoyer sur le topic ' groupe-six-topic-reader '.

     storm/bin/storm jar project-bda-1.0-SNAPSHOT-jar-with-dependencies.jar Producer /path/file/csv


### Topologie
Chaque topologie consomme les données depuis le topic ' groupe-six-topic-reader ' , fait son traitement et envoie le résultat
sur le topic 'AlertTopicGroupSix'.

Lancement des topologies sur la VM

NoBicycleTopology: il n'y a plus de vélos disponibles.

    storm/bin/storm jar project-bda-1.0-SNAPSHOT-jar-with-dependencies.jar topology.NoBicycleTopology 

NoAvailableBikeStandTopology: il n'y a plus de places disponibles.

    storm/bin/storm jar project-bda-1.0-SNAPSHOT-jar-with-dependencies.jar topology.NoAvailableBikeStandTopology

FivePlaceLast3HoursTopo:lLa disponibilité en vélos d'une stations sur les 3 dernières heures est inférieure à 5.

    storm/bin/storm jar project-bda-1.0-SNAPSHOT-jar-with-dependencies.jar topology.FivePlaceLast3HoursTopo
    
### Consommateur
Le consommateur lit les données sur le topic 'AlertTopicGroupSix' et les stocke sur HIVE.

    storm/bin/storm jar project-bda-1.0-SNAPSHOT-jar-with-dependencies.jar Consumer
