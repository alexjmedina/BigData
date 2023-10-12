""" Programme pour compter le nombre d'évaluation 5* par film et pour trier les films par nombre d'évaluations 5*"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteEvaluationsFilmMaximum5Etoiles(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evaluations,
                   reducer=self.reducer_compte_evaluations),
            MRStep(reducer=self.reducer_maximum_evaluations)       ]

    def mapper_get_evaluations(self, _, line):
        
        colonnes = line.split("\t")
        film = colonnes[1]
        eval = colonnes[2]
        if eval=="5" :
               yield film, 1

#Le tri étant par défaut en mode texte ; on aligne les nombres à droite et on complète par des zéro sur 3 positions max            
    def reducer_compte_evaluations(self, film, uns):
         yield  "key", [sum(uns), film]

    def reducer_maximum_evaluations(self, _, evaluations):
            evaluations = [x for x in evaluations]
            yield    "maximum est", max(evaluations)        
if __name__ == '__main__':
    CompteEvaluationsFilmMaximum5Etoiles.run()