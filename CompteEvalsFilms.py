""" Programme pour compter les nombres de evaluations du fichier u.data """
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteEvals(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evals,
                   reducer=self.reducer_compte_evals)       ]

    def mapper_get_evals(self, _, line):
        colonnes = line.split("\t")
        evals = colonnes[2]
        if evals=="5":
            films = colonnes[1]
            if films=="900":
                yield films, 1

    def reducer_compte_evals(self, eval, uns):
        yield eval, sum(uns)

if __name__ == '__main__':
    CompteEvals.run()
