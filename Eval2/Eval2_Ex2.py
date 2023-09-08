""" Evaluation2 - Exercice 2 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class FiltreNombresPairs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres_pairs,
                   reducer=self.reducer_compte_nombres_pairs)       ]

    def mapper_get_nombres_pairs(self, _, line):
        nombres = int(line)
        if nombres % 2 == 0:
            yield(nombres, 1)
            #yield(nombres, None)

    def reducer_compte_nombres_pairs(self, key, values):
        yield(key, sum(values))
        #yield(key, None)

if __name__ == '__main__':
    FiltreNombresPairs.run()