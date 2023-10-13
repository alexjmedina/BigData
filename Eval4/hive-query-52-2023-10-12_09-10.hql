create database if not exists Ev4_AMedina;

use Ev4_AMedina;

create table if not exists  Ev4_AMedina_Collisions1 (No_seq_coll string, dt_accdn string, rue_accdn string, 
accdn_pres_de string, cd_genre_accdn int, nb_blesses_graves int, nb_blesses_legers int, gravite string)
row format delimited fields terminated by ',';

load data local inpath '/tmp/Ev4/CollisionsPartie1AS.txt' overwrite into table Ev4_AMedina_Collisions1;

create table if not exists  Ev4_AMedina_Genres (cd_genre_accdn int,Description string)
row format delimited fields terminated by ',';

load data local inpath '/tmp/Ev4/genres_collisions.txt' overwrite into table Ev4_AMedina_Genres;

create table if not exists  Ev4_AMedina_Collisions2 (No_seq_coll string, dt_accdn string, rue_accdn string, 
accdn_pres_de string, cd_genre_accdn int, blesses map<string,int>, gravite string)
row format delimited fields terminated by ','
collection items terminated by "#"
map keys terminated by ":";

load data local inpath '/tmp/Ev4/Collisions2.txt' overwrite into table Ev4_AMedina_Collisions2;