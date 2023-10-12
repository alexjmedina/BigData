from mrjob.job import MRJob
from mrjob.step import MRStep

class TransformVentes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ventes,
                   reducer=self.reducer_sommaire)       ]

    def mapper_get_ventes(self, _, line):
        colonnes = line.split(";")
        
        #
        poste = colonnes[0]
        trim = colonnes[1]
        ventes = colonnes[2]
        yield poste, [trim, ventes]

    def reducer_sommaire(self, poste, sommaire):
        sommaire = [x for x in sommaire]
        
        for element in sommaire:
            
        # 
        yield poste, 

if __name__ == '__main__':
    TransformVentes.run()