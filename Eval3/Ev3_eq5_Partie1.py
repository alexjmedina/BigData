""" Evaluation3 - Partie 1 - Question 2 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class NettoyerLesDonnees(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_mots,
                   reducer=self.reducer_get_results)       ]

    def mapper_get_mots(self, _, line):
        colonnes = line.split(' - ')
        livre_title = colonnes[0]
        auteur = colonnes[1]
        langue_original = colonnes[2]
        title_original = colonnes[3]
        if auteur != "inconnu" or langue_original != "inconnu" or title_original !="inconnu":
            if langue_original.strip().lower() == "français":
                yield 1, [livre_title, auteur, langue_original, title_original]
            else:
                yield 1, [livre_title]

    def reducer_get_results(self, key, books):
        for book in books:
            yield key, book

if __name__ == '__main__':
    NettoyerLesDonnees.run()