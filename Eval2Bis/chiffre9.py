#Programme pour afficher les nombres qui terminent par 9 - Nombres.txt

from mrjob.job import MRJob
from mrjob.step import MRStep

class AfficherNombresChiffre9(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_chiffre9,
                   reducer=self.reducer_chiffre9)       ]

    def mapper_get_chiffre9(self, _, line): #Lire un nombre
        nombre = int(line)
        if nombre % 10 == 9:
            yield  nombre, None
        
    """Cette méthode est utilisée pour effectuer la première étape de la tâche MapReduce. Elle prend deux arguments : _ (qui n'est pas utilisé) et line. 
    La variable line contient une ligne de données, qui est supposée être un nombre.
    La méthode convertit la ligne en un nombre entier avec int(line).
    Ensuite, elle vérifie si le nombre se termine par le chiffre 9 en effectuant une opération de modulo (nombre % 10 == 9). 
    Si c'est le cas, elle émet une paire clé-valeur où la clé est le nombre se terminant par 9, et la valeur est None."""
    
    def reducer_chiffre9(self,nombre,_): 
        yield "nombre se terminant par 9: ",nombre
    
    """Cette méthode est utilisée pour effectuer la deuxième étape de la tâche MapReduce, qui est la réduction. Cependant, dans ce cas, 
    la réduction ne fait rien de particulier, elle se contente de regrouper les nombres se terminant par 9 ensemble.
    Elle prend deux arguments, nombre et _, bien que le caractère _ ne soit pas utilisé.
    Elle émet une paire clé-valeur où la clé est une chaîne de caractères "nombre se terminant par 9: " et la valeur est le nombre lui-même."""

if __name__ == '__main__':
    AfficherNombresChiffre9.run()