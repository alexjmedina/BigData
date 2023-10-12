from mrjob.job import MRJob
from mrjob.step import MRStep

class AuteursPlusQuUnLivre(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteurs,
                   reducer=self.reducer_get_livres)#,
            #MRStep(reducer=self.reducer_output_books)
        ]

    def mapper_get_auteurs(self, _, line):
        data = line.split(',')
        livre = data[1]
        auteur = data[2].zfill(2)
        yield auteur, livre

    def reducer_get_livres(self, auteur, livres):
        livres_list = list(livres)
        if len(livres_list) > 1:
            #yield auteur.lstrip('0'), livres_list
            yield int(auteur), livres_list

if __name__ == '__main__':
    AuteursPlusQuUnLivre.run()