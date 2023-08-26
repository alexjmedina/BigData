from mrjob.job import MRJob
from mrjob.step import MRStep

class JointurePersonnesPays3(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_personnespays,
                   reducer=self.reducer_jointure)       ]

    def mapper_get_personnespays(self, _, line):
        colonnes = line.split("|")
        
        # Check if record has 2 columns (country data)
        if len(colonnes) == 2:
            pays = colonnes[0]
            pays_code = colonnes[1]
            yield pays_code, [pays]

        # Check if record has 3 columns (person data)
        else:
            prenom = colonnes[0]
            nom = colonnes[1]
            pays_code = colonnes[2]
            yield pays_code, [prenom,nom]
                     

    def reducer_jointure(self, pays_code, personnesPays):
        personnesPays = [x for x in personnesPays]
        
        pays = pays_code
        for element in personnesPays:
            if len(element) == 1:
                pays = element
                break
        personnesPays.remove(pays)

        # Compte les personnes par pays
        yield pays, len(personnesPays)

if __name__ == '__main__':
    JointurePersonnesPays3.run()