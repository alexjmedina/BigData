from mrjob.job import MRJob
from mrjob.step import MRStep

class AuteursPlusQuUnLivre(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteurs,
                   reducer=self.reducer_get_livres),
            MRStep(reducer=self.reducer_get_auteurs_plus_qu_un)
        ]

    def mapper_get_auteurs(self, _, line):
        data = line.split(',')
        numero = data[0]
        livre = data[1]
        auteur = data[2]
        yield auteur, (numero, livre)

    def reducer_get_livres(self, auteur, livres):
        livres_list = list(livres)
        if len(livres_list) > 1:
            yield auteur, livres_list

    def reducer_get_auteurs_plus_qu_un(self, auteur, livres):
        for livre in livres:
            yield auteur, livre

if __name__ == '__main__':
    AuteursPlusQuUnLivre.run()