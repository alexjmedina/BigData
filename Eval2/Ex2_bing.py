from mrjob.job import MRJob

class FilterEvenNumbers(MRJob):
    def mapper(self, _, line):
        number = int(line)
        if number % 2 == 0:
            yield(number, None)

    def reducer(self, key, values):
        yield(key, None)

if __name__ == '__main__':
    FilterEvenNumbers.run()