""" Program for transform Ventes in SommairePoste """
from mrjob.job import MRJob
from mrjob.step import MRStep

class TransformVentes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_poste,
                   reducer=self.reducer_sommaire),
            MRStep(reducer=self.reducer_secteur)       ]

    def mapper_get_poste(self, _, line):
        colonnes = line.split(";")
        
        if len(colonnes) == 2:
            poste = colonnes[0]
            secteur = colonnes[1]
            yield poste, secteur
        else:
            poste = colonnes[0]
            trim = colonnes[1]
            ventes = colonnes[2]
            yield poste, [trim,ventes]

    def reducer_sommaire(self, poste, trimventes):
        trimventes = [x for x in trimventes]
        yield poste, trimventes

    def reducer_secteur(self, poste, secteur):
        secteur = [x for x in secteur]
        yield poste, secteur

if __name__ == '__main__':
    TransformVentes.run()