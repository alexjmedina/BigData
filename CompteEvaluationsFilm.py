""" Programme pour compter les mots d'un texte et afficher pour chaque mot le nombre d'occurence"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteEvaluationsFilm(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evaluations,
                   reducer=self.reducer_compte_evaluations)       ]

    def mapper_get_evaluations(self, _, line):
         
        colonnes = line.split("\t")
        film = colonnes[1]
        yield film, 1

    def reducer_compte_evaluations(self, eval, uns):
        yield eval, sum(uns)

if __name__ == '__main__':
    CompteEvaluationsFilm.run()
