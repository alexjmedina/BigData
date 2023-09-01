""" Evaluation2 - Exercice 1 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteNombres(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres,
                   reducer=self.reducer_compte_nombres)       ]

    def mapper_get_nombres(self, _, line):
        nombres = line
        yield nombres, 1

    def reducer_compte_nombres(self, nombres, uns):
        yield nombres, sum(uns)

if __name__ == '__main__':
    CompteNombres.run()
