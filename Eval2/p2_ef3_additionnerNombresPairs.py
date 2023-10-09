""" Programme pour calculer la somme des nombres pairs """
from mrjob.job import MRJob
from mrjob.step import MRStep

class additionnerNombresPairs(MRJob):
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
        yield "Somme nombres pairs :", sum(nombresPairs) #calculer la somme des nombres pairs
    

if __name__ == '__main__':
    additionnerNombresPairs.run()

""" commande pour exécuter le programme
py p2_ef3_additionnerNombresPairs.py Nombres.txt 
"""