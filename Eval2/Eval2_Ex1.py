""" Evaluation2 - Exercice 1 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteNombresPairs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres_pairs,
                   reducer=self.reducer_compte_nombres_pairs)       ]

    def mapper_get_nombres_pairs(self, _, line):
        nombres = int(line)
        if nombres % 2 == 0:
            yield("Total even numbers", 1)

    def reducer_compte_nombres_pairs(self, key, values):
        #values = [x for x in values] #transform generator in list
        yield key, sum(values)

if __name__ == '__main__':
    CompteNombresPairs.run()
