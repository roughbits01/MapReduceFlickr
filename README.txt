Projet de AZOUZI Marwen et FAURE Adrien

Voici le compte rendu de notre projet hadoop - Wordcount and flicker

Cette archive contient les dossiers suivants :

    ├── lib
    ├── rapport
    ├── README.md
    ├── README.txt
    ├── rapport.pdf
    └── src

Le dossier rapport contient les sources de notre rapport.
Le dossier src contients le sources de notre TP.
Il est composé des fichiers suivant :

    src
    ├── Country.java
    ├── PairString.java
    ├── PairStringOneShot.java
    ├── Question2_1.java
    ├── Question3_1.java
    ├── Question3_2.java
    ├── StringAndInt.java
    ├── WordCountCombiner.java
    └── WordCount.java

* La classes WordCount contient la reponce à la partie un du TP, elle comprends donc la version final avec le in-mapper;
* La classe WordCountCombiner contient la version intermediaire du wordcount avec le Combiner;
* La classe Question2_1 contient la version "in-memory" du compte-tag de flicker;
* La classe Question3_1 contient la version en deux job du compte-tag de flicker;
* La classe Question3_2 contient la version en une passe du compte-tag de flicker.
