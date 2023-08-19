""" Programme pour compter le nombre d'évaluation 5* par film et pour trier les films par nombre d'évaluations 5*"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class JointureItemData(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evaluations,
                   reducer=self.reducer_compte_evaluations)       ]

    def mapper_get_evaluations(self, _, line):

        if "|" in line :
             colonnes = line.split("|")
             film_id = colonnes[0]
             film_name_and_eval = colonnes[1]
             yield film_id, film_name_and_eval
        else:
             colonnes = line.split("\t")
             film_id = colonnes[1]
             eval = colonnes[2]
             yield film_id, eval
             

    def reducer_compte_evaluations(self, film_id, film_name_and_eval):
        film_name_and_eval = [x for x in film_name_and_eval]
        title = film_name_and_eval[0]
        for element in film_name_and_eval[1:]:
            yield title, element

if __name__ == '__main__':
    JointureItemData.run()