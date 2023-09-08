from mrjob.job import MRJob

class SumEvenNumbers(MRJob):
    def mapper(self, _, line):
        number = int(line)
        if number % 2 == 0:
            yield("Sum of even numbers", number)

    def reducer(self, key, values):
        yield(key, sum(values))

if __name__ == '__main__':
    SumEvenNumbers.run()