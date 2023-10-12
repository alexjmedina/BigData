from mrjob.job import MRJob
from mrjob.step import MRStep

class JointurePersonnesPays(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_personnespays,
                   reducer=self.reducer_jointure)       ]

    def mapper_get_personnespays(self, _, line):
        colonnes = line.split("|")
        
        # Check if record has 2 columns (country data)
        if len(colonnes) == 2:
            pays, pays_code = colonnes
            yield pays_code, ('country', pays)

        # Check if record has 3 columns (person data)
        elif len(colonnes) == 3:
            prenom, nom, pays_code = colonnes
            yield pays_code, ('person', prenom, nom)
                     

    def reducer_jointure(self, pays_code, values):
        country_name = None
        persons = []

        for value in values:
            if value[0] == 'country':
                country_name = value[1]
            else:
                persons.append(value[1:])

        for person in persons:
            prenom, nom = person
            yield (prenom, nom, country_name), pays_code

if __name__ == '__main__':
    JointurePersonnesPays.run()