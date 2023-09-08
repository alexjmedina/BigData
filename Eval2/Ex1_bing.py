from mrjob.job import MRJob

class CountEvenNumbers(MRJob):
    def mapper(self, _, line):
        number = int(line)
        if number % 2 == 0:
            yield("Even numbers", 1)

    def reducer(self, key, values):
        yield(key, sum(values))

if __name__ == '__main__':
    CountEvenNumbers.run()