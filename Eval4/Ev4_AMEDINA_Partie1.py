from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import BytesValueProtocol

class NettoyerDonnees(MRJob):
    OUTPUT_PROTOCOL = BytesValueProtocol
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_result1,
                   reducer=self.reducer_get_result2)       ]

    def mapper_get_result1(self, _, line):
        
        colonnes = line.split(',')
        No_seq_coll         =colonnes[0]
        dt_accdn            =colonnes[2]
        rue_accdn           =colonnes[7]
        accdn_pres_de       =colonnes[9]
        cd_genre_accdn      =colonnes[11]
        nb_blesses_graves   =colonnes[29]
        nb_blesses_legers   =colonnes[30]
        gravite             =colonnes[34]

        cd_genre_accdn=cd_genre_accdn.replace('"','')
        nb_blesses_graves=nb_blesses_graves.replace('"','')
        nb_blesses_legers=nb_blesses_legers.replace('"','')
        
        yield dt_accdn, [No_seq_coll, rue_accdn, accdn_pres_de, cd_genre_accdn, nb_blesses_graves, nb_blesses_legers, gravite]
        
    def reducer_get_result2(self, key, value):
        yield key, bytes(value, 'utf-8')

if __name__ == '__main__':
    NettoyerDonnees.run()