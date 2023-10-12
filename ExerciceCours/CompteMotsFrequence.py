""" Programme pour compter les mots d'un texte et et les afficher en ordre croissant selon la fréquence"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteMots(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_mots,
                   reducer=self.reducer_compte_mots),
            MRStep(reducer=self.reducer_frequence_mots)    ]
    def mapper_get_mots(self, _, line):
        line = line.strip()
        line = line.lower()
        line = line.replace(","," ")
        line = line.replace("."," ")
        line = line.replace(";"," ")
        line = line.replace("?"," ")
        line = line.replace("!"," ")
        line = line.replace(":"," ")      

        mots = line.split()
        for mot in mots:
            yield mot, 1
    def reducer_compte_mots(self, mot, uns):
        yield  str(sum(uns)).zfill(3), mot
    def reducer_frequence_mots(self, mot, frequences):
        for frequence in frequences :
            yield  frequence, mot
if __name__ == '__main__':
    CompteMots.run()
