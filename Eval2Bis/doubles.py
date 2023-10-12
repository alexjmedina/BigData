""" Programme pour afficher les nombres que ont des doublons """

from mrjob.job import MRJob
from mrjob.step import MRStep

class AfficherNombresDoublons(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_doublons,
                   reducer=self.reducer_nombres_doublons)       ]

    def mapper_get_doublons(self, _, line): #Lire un nombre
        nombre = int(line)
        yield  nombre, 1
    
    """Le mapper prend deux arguments, _ (qui est ignoré car nous ne l'utilisons pas) et line, qui est une ligne du fichier d'entrée. 
    Le mapper convertit cette ligne en un nombre entier et émet une paire clé-valeur où la clé est le nombre et la valeur est 1."""

    def reducer_nombres_doublons(self,nombre,compteur): 
        total = sum(compteur)
        if total > 1:
            yield nombre,total
    
    """Le reducer prend une clé (le nombre) et une liste de compteurs associée à cette clé. Il additionne les compteurs pour obtenir le total d'occurrences de ce nombre. 
    Si le total est supérieur à 1 (c'est-à-dire s'il y a un doublon), le reducer émet à nouveau la clé (le nombre) et le total."""
    
if __name__ == '__main__':
    AfficherNombresDoublons.run()

