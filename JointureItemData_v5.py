""" Programme pour compter le nombre d'évaluation 5* par film et pour trier les films par nombre d'évaluations 5*"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class JointureItemData_v4(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evaluations,
                   reducer=self.reducer_compte_evaluations),
            MRStep(reducer=self.reducer_tri_evaluations)       ]

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
             if eval == "5":
                yield film_id, eval
             

    def reducer_compte_evaluations(self, film_id, film_name_and_eval):
        film_name_and_eval = [x for x in film_name_and_eval]
        for element in film_name_and_eval:
            if not element.isdigit():
                title = element
                break
        
        film_name_and_eval.remove(title)

        yield str(len(film_name_and_eval)).zfill(3), title

    def reducer_tri_evaluations(self, eval, listeFilms):
        listeFilms = [x for x in listeFilms]
        for film in listeFilms:
            yield film, eval

if __name__ == '__main__':
    JointureItemData_v4.run()