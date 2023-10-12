create database if not exists LibrairieEv3Eq5;

use LibrairieEv3Eq5;

create table if not exists  livresev3eq5 (nolivre int, languetitre map<string,string>, nomauteur array<string>)
row format delimited fields terminated by ','
collection items terminated by "#"
map keys terminated by ":";

-- charger les donnees des livres
load data local inpath '/tmp/Librairie/LivresPartie2_eq5.txt' overwrite into table livresev3eq5;
