""" Evaluation3 - Partie 1 - Question 2 - Equipe 5 """
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol

class NettoyerLesDonnees(MRJob):
    OUTPUT_PROTOCOL = BytesValueProtocol
    
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

        if "inconnu" in auteur:
            auteur = ""

        if "inconnu" in langue_original:
            langue_original = ""

        if "inconnu" in title_original:
            title_original = ""

        if "français" in langue_original:
            langue_original = ""

        if "français" in title_original:
            title_original = ""

        yield 1, [ livre_title, auteur, langue_original, title_original]
        
    def reducer_get_results(self, _, books):
        book_id = 1
        with open("C:/Users/Utilisateur/Documents/GitHub/BigData/Eval3/LivresPartie1_eq5.txt",'w',encoding="utf-8") as f:
            for book in books:
                yield _, bytes( f"{book_id},{','.join(book)}",  'utf-8')
                f.write(f"{book_id},{','.join(book)}")
                f.write("\n")
                book_id += 1

if __name__ == '__main__':
    NettoyerLesDonnees.run()