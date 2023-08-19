""" Programme pour faire la jointure de u.Item et u.data et afficher pour chaque film, son titre et ses évaluations"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class JointureItemdata3(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_evaluations,
                   reducer=self.reducer_compte_evaluations)       ]

#lecture des fichiers u.item et u.data dans cet ordre
    def mapper_get_evaluations(self, _, line):
        
        #est-ce une ligne u.item
        if "|" in line:
            colonnes = line.split("|")
            film = colonnes[0]
            titre = colonnes[1]
            yield film, titre
        else: #c'est une ligne data
            colonnes = line.split("\t")
            film = colonnes[1]
            eval = colonnes[2]
            yield film, eval
        
#afficher les évaluations des films
    def reducer_compte_evaluations(self, film, titresEvals):
        titresEvals = [x for x in titresEvals]
           
           #rechercher le titre
        for element in titresEvals:
            if not element.isdigit():
                titre = element
                break

            #enlever le titre de la liste
        titresEvals.remove(titre) 

           #parcourir les évaluations
        for element in titresEvals:
            yield titre, element



if __name__ == '__main__':
    JointureItemdata3.run()
