""" Program for transform Ventes in SommairePoste """
from mrjob.job import MRJob
from mrjob.step import MRStep

class TransformVentes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ventes,
                   reducer=self.reducer_sommaire)
        ]

    def mapper_get_ventes(self, _, line):
        colonnes = line.split(";")
        
        poste = colonnes[0]
        trim = int(colonnes[1])
        ventes = float(colonnes[2].replace(" ", "").replace("$", ""))
        yield poste, (trim, ventes)

    def reducer_sommaire(self, poste, sommaire):
        ventes_trim = [0, 0, 0, 0]
        
        for element in sommaire:
            trim = element[0]
            ventes = element[1]
            ventes_trim[trim - 1] += ventes
        
        yield poste, ventes_trim

if __name__ == '__main__':
    TransformVentes.run()