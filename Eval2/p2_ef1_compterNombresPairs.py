""" Programme pour filtrer les nombres pairs """
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompterNombresPairs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres,
                   reducer=self.reducer_nombres_pairs)       ]

    def mapper_get_nombres(self, _, line): #Lire un nombre
        nombre = int(line)
        if nombre % 2 == 0 : #Vérifier si le nombre est pair
            yield None, nombre  #Utiliser une clé unique (None) pour toutes les paires générées 
                                    #afin de constituer une liste unique pour le reducer

    def reducer_nombres_pairs(self,_, nombresPairs): #nombresPairs contient la liste de tous les nombres pairs trouvés par les mappers
        nombresPairs = [x for x in nombresPairs] #transformer le générateur nombre en une liste pour permettre d'utiliser len par la suite
        yield "nombre nombres pairs :", len(nombresPairs) #La taille de la liste donne le nombre de nombres pairs
    

if __name__ == '__main__':
    CompterNombresPairs.run()


""" commande pour exécuter le programme
py p2_ef1_compterNombresPairs.py Nombres.txt 
"""