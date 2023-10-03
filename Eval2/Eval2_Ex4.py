""" Evaluation2 - Exercice 4 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class jointureAuteursLivres(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteurs_livres,
                   reducer=self.reducer_auteurs_livres),
            MRStep(reducer=self.reducer_tri_auteurs_livres)]

    def mapper_get_nombres_pairs(self, _, line):
        nombres = int(line)
        if nombres % 2 == 0:
            yield("Sum of even numbers", nombres)

    def reducer_compte_nombres_pairs(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    jointureAuteursLivres.run()