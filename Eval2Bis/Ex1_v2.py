""" Evaluation2 Bis - Exercice 1 """
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteNombresMultiples(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nombres_multiples,
                   reducer=self.reducer_compte_nombres_multiples)
        ]

    def mapper_get_nombres_multiples(self, _, line):
        nombres = int(line)
        if nombres % 3 == 0 and nombres % 5 == 0:
            yield(None, nombres)

    def reducer_compte_nombres_multiples(self, _, nombres):
        nombres = list(map(int, nombres))
        yield "Nombres multiples de 3 et 5:", nombres
        yield "Somme total des nombres multiples de 3 et 5:", sum(nombres)
        yield "Nombre de nombres multiples de 3 et 5:", len(nombres)

if __name__ == '__main__':
    CompteNombresMultiples.run()