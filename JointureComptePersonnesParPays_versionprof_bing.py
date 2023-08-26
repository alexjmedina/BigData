from mrjob.job import MRJob
from mrjob.step import MRStep

class JointureComptePersonnesParPays(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_personnespays,
                   reducer=self.reducer_jointure)
        ]

    def mapper_get_personnespays(self, _, line):
        colonnes = line.split("|")
        
        # Check if record is country data
        if colonnes[1].isalpha() and len(colonnes[1]) == 2:
            pays = colonnes[0]
            pays_code = colonnes[1]
            yield pays_code, [pays]

        # Check if record is person data
        else:
            prenom = colonnes[0]
            nom = colonnes[1]
            pays_code = colonnes[2]
            yield pays_code, [prenom,nom]

    def reducer_jointure(self, pays_code, personnesPays):
        personnesPays = [x for x in personnesPays]
        
        pays = [x for x in personnesPays if len(x) == 1][0]
        personnes = [x for x in personnesPays if len(x) > 1]

        yield pays, len(personnes)

if __name__ == '__main__':
    JointureComptePersonnesParPays.run()
