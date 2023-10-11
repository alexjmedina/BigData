""" Evaluation3 - Partie 2 - Equipe 5 """
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol

class TransformerLesDonnees(MRJob):
    OUTPUT_PROTOCOL = BytesValueProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_mots,
                   reducer=self.reducer_get_results)       ]

    def mapper_get_mots(self, _, line):
        colonnes = line.split(',')
       
        livre_title = colonnes[1]
        auteur = colonnes[2]
        langue_original = colonnes[3]
        title_original = colonnes[4]
        
        nom_fr = livre_title
        nom_an = ""
        if langue_original != "":
            var1 = langue_original.split(" : ")
            var2 = title_original.split(": ")
            nom_an = var1[1]+":"+var2[1]
            if nom_an != "":
                nom_an = "#" + nom_an

        # map array
        map_title = "Français:" + nom_fr + nom_an 

        # auteur en array
        arr_auteur = auteur.replace(" et ","#")
        
        # resultat de mapper
        yield 1, [ map_title, arr_auteur]

    def reducer_get_results(self, _, books):
        book_id = 1
        with open("C:/Users/Utilisateur/Documents/GitHub/BigData/Eval3/LivresPartie2_eq5.txt",'w',encoding="utf-8") as f:
            for book in books:
                yield _, bytes( f"{book_id},{','.join(book)}",  'utf-8')
                f.write(f"{book_id},{','.join(book)}")
                f.write("\n")
                book_id += 1

if __name__ == '__main__':
    TransformerLesDonnees.run()