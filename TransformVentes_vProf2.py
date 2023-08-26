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
        trim = colonnes[1]
        ventes = colonnes[2]
        yield poste, [trim,ventes]

    def reducer_sommaire(self, poste, trimventes):
        trimventes = [x for x in trimventes]
        yield poste, trimventes

if __name__ == '__main__':
    TransformVentes.run()