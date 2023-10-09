""" Programme pour filtrer les nombres pairs """
from mrjob.job import MRJob
from mrjob.step import MRStep

class jointureAuteursLivres(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteur_livre,
                   reducer=self.reducer_livresAuteur),
            MRStep(reducer=self.reducer_tri_livresAuteur)]

    #lecture des fichiers auteurs.txt et livres.txt
    def mapper_get_auteur_livre(self, _, line):
        colonnes = line.split(",") 
        if len(colonnes) == 2: # vérifier si la laligne est celle d'un auteur
            numero_auteur = colonnes[0]
            nom_auteur = colonnes[1]
            yield numero_auteur , [nom_auteur] # mettre l'autre dans une liste de longueur 1 
                                                #afin de le retrouver facilement lors de l'étape Reduce
        else : # c'est une ligne de livre
            numero_livre = colonnes[0]
            titre_livre = colonnes[1]
            numero_auteur = colonnes[2]
            yield numero_auteur, [numero_livre, titre_livre]

    def reducer_livresAuteur(self,numero_auteur, livresAuteur): #livresAuteur contient un auteur et les livres écrits par cet auteur
        livresAuteur = [x for x in livresAuteur] #transformer le générateur en liste pour parcourir cette liste par la suite
        auteur = [numero_auteur] # initialiser auteur par [numero_auteur] car il se pourrait que ce numéro 
                                 #ne correspondent à aucun nom auteur !
        for element in livresAuteur :
            if len(element) == 1: #Vérifier si l'élément correspond à un auteur car l'élément auteur se trouve 
                #dans une liste à un élément et l'élément livre se trouve dans une liste à 2 éléments                
                auteur = element
                break # il y a un seul auteur dans la liste, si on le trouve, nous n'avons plus besoin de continuer le parcours de la liste
        
        try:
            livresAuteur.remove(auteur) # Si l'on a trouvé un auteur on l'enlève de la liste
        except ValueError :
            pass
        
        for element in livresAuteur : # pour chaque livre on génère une paire contenant le numéro de livre (element[0]) comme clé 
            #et une liste composée du numéro de livre, titre du livre (element[1]) et le nom de l'auteur (auteur[0])
            #zfill est utilisé pour trier les numéros de livre par la suite pour le Reducer
            yield element[0].zfill(3), [element[0], element[1], auteur[0]] #

    def reducer_tri_livresAuteur(self,numero_livre, livre): #le générateur livre contient 
                                            #les informations (numéro, titre, auteur) d'un seul livre
        livre = [x for x in livre] #On transforme le générateur en une liste pour l'afficher par la suite
        yield numero_livre,livre[0] # afficher le contenu de la liste livre
        

if __name__ == '__main__':
    jointureAuteursLivres.run()

""" commande pour exécuter le programme
py p2_em4_jointureAuteursLivres.py Auteurs.txt Livres.txt
"""