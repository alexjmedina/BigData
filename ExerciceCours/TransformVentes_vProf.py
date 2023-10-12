""" Program for transform Ventes in SommairePoste """
from mrjob.job import MRJob
from mrjob.step import MRStep

class TransformVentes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_poste,
                   reducer=self.reducer_sommaire)
        ]

    def mapper_get_poste(self, _, line):
        colonnes = line.split(";")
        
        poste = colonnes[0]
        ventes = colonnes[2]
        yield poste, ventes

    def reducer_sommaire(self, poste, ventes):
        ventes = [x for x in ventes]
        yield poste, ventes

if __name__ == '__main__':
    TransformVentes.run()