from mrjob.job import MRJob
from mrjob.step import MRStep

class JointurePaysPersonnes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_pays,
                   reducer=self.reducer_compte_pays)       ]

    def mapper_get_pays(self, _, line):

        colonnes = line.split("|")
        prenom = colonnes[0]
        nom = colonnes[1]
        pays_code = colonnes[2]
        pays = colonnes[3]
        prenom_nom_pcode = prenom + ' ' + nom + ' ' + pays_code
        yield pays, prenom_nom_pcode
                     

    def reducer_compte_pays(self, pays, prenom_nom_pcode):
        prenom_nom_pcode = [x for x in prenom_nom_pcode]
        title = prenom_nom_pcode[0]
        for element in prenom_nom_pcode[1:]:
            yield title, element

if __name__ == '__main__':
    JointurePaysPersonnes.run()