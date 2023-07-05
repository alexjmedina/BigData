""" Programme pour compter les mots d'un texte et afficher pour chaque mot le nombre d'occurence"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class CompteMots(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_mots,
                   reducer=self.reducer_compte_mots)       ]

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
        yield mot, sum(uns)

if __name__ == '__main__':
    CompteMots.run()
