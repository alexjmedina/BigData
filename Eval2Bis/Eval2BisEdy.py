""" Programme pour afficher les noms des auteurs triés par ordre ascendant - auteurs.txt"""

"""Em resumo, este código define um trabalho MapReduce que processa linhas de um arquivo CSV, calcula o comprimento dos nomes dos autores e, em seguida, classifica os autores com base no comprimento de seus nomes. 
As etapas de mapeamento e redução são definidas nos métodos mapper_get_auteurs, reducer_noms_auteurs e reducer_tri_compteurs."""

from mrjob.job import MRJob
from mrjob.step import MRStep

class AfficherNomAuteurs(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_auteurs,
                   reducer=self.reducer_noms_auteurs),
            MRStep(reducer=self.reducer_tri_compteurs)       ]
    
    """O método mapper_get_auteurs é chamado na primeira etapa. Ele recebe uma linha de entrada (uma linha do arquivo CSV) e a divide em itens com base na vírgula. 
    Em seguida, ele pega o nome do autor (localizado na segunda posição, indexada como 1)
    e emite um par chave-valor, onde a chave é o nome do autor e o valor é o comprimento do nome."""

    def mapper_get_auteurs(self, _, line):
        items = line.split(",")  
        auteur =items[1]
        yield  auteur,len(auteur)
        
    """O método reducer_noms_auteurs é chamado na primeira etapa de redução. Ele recebe uma lista de comprimentos de nomes para um autor e emite um par chave-valor. 
    A chave é o somatório dos comprimentos de todos os nomes (preenchido com zeros à esquerda para garantir um comprimento fixo de 2 dígitos), e o valor é o nome do autor."""

    def reducer_noms_auteurs(self,auteur,longueur): 
        ##auteur = [x for x in auteur]
        yield str(sum(longueur)).zfill(2),auteur
 
    """O método reducer_tri_compteurs é chamado na segunda etapa de redução. Ele classifica os autores com base no comprimento de seus nomes e emite pares chave-valor, 
    onde a chave é o comprimento do nome e o valor é o nome do autor."""

    def reducer_tri_compteurs(self, longueur, auteur):
        for eval in auteur :
            yield    int(longueur),eval

if __name__ == '__main__':
    AfficherNomAuteurs.run()

