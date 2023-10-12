from mrjob.job import MRJob
from mrjob.step import MRStep

class JointurePaysPersonnes(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_records, reducer=self.join_by_country_code)
        ]

    def map_records(self, _, line):
        colonnes = line.split("|")
        
        # Country data
        if len(colonnes) == 2:
            pays, pays_code = colonnes
            yield pays_code, ('country', pays)

        # Person data
        elif len(colonnes) == 3:
            prenom, nom, pays_code = colonnes
            yield pays_code, ('person', prenom, nom)

    def join_by_country_code(self, pays_code, values):
        country_name = None
        for value in values:
            if value[0] == 'country':
                country_name = value[1]
            else:  # 'person' record
                prenom, nom = value[1:]
                yield (prenom, nom, country_name), pays_code

if __name__ == '__main__':
    JointurePaysPersonnes.run()