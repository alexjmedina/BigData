from mrjob.job import MRJob
from mrjob.step import MRStep

class AuteursPlusQuUnLivre(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteurs,
                   reducer=self.reducer_get_livres),
            MRStep(mapper=self.mapper_sort_by_id,
                   reducer=self.reducer_output_books)
        ]

    def mapper_get_auteurs(self, _, line):
        data = line.split(',')
        livre = data[1]
        auteur = data[2]
        yield auteur, livre

    def reducer_get_livres(self, auteur, livres):
        livres_list = list(livres)
        if len(livres_list) > 1:
            yield auteur, livres_list

    def mapper_sort_by_id(self, auteur, livres):
        yield int(auteur), livres

    def reducer_output_books(self, auteur, livres):
        for livre in livres:
            yield auteur, livre

if __name__ == '__main__':
    AuteursPlusQuUnLivre.run()