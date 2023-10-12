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
        auteur = colonnes[1].lower()
        langue_original = colonnes[2].lower()
        title_original = colonnes[3].lower()
        if "inconnu" not in auteur and "inconnu" not in langue_original and "inconnu" not in title_original:
            if "français" in langue_original:
                yield 1, [livre_title, auteur]
            else:
                yield 1, [livre_title, auteur, langue_original, title_original]

    def reducer_get_results(self, key, books):
        for book in books:
            yield key, book

if __name__ == '__main__':
    NettoyerLesDonnees.run()