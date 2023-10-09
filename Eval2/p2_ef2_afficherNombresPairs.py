""" Programme pour filtrer les nombres pairs """
from mrjob.job import MRJob
from mrjob.step import MRStep

class AfficherNombresPairs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres,
                   reducer=self.reducer_nombres_pairs)       ]

    def mapper_get_nombres(self, _, line): #Lire un nombre
        nombre = int(line)
        if nombre % 2 == 0 : #Vérifier si le nombre est pair
            yield None, nombre  #Utiliser une clé unique (None) pour toutes les paires générées 
                                    #afin de constituer une liste unique pour le reducer

    def reducer_nombres_pairs(self,_, nombresPairs): #nombresPairs contient la liste de tous les nombres pairs trouvées par les Mappers
        nombresPairs = [x for x in nombresPairs] #transformer le générateur nombre en une liste pour permettre de l'afficher par la suite
        yield "nombres pairs :", nombresPairs #afficher les nombres pairs
    

if __name__ == '__main__':
    AfficherNombresPairs.run()

""" commande pour exécuter le programme
py p2_ef2_AfficherNombresPairs.py Nombres.txt 
"""