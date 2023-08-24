from mrjob.job import MRJob
from mrjob.step import MRStep

class JointurePaysPersonnes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_pays,
                   reducer=self.reducer_compte_pays)       ]

    def mapper_get_pays(self, _, line):

        if "|" in line :
             colonnes = line.split("|")
             prenom = colonnes[0]
             nom = colonnes[1]
             pays_code = colonnes[2]
             yield pays_code, prenom_nom
                     

    def reducer_compte_pays(self, film_id, film_name_and_eval):
        film_name_and_eval = [x for x in film_name_and_eval]
        title = film_name_and_eval[0]
        for element in film_name_and_eval[1:]:
            yield title, element

if __name__ == '__main__':
    JointurePaysPersonnes.run()