from mrjob.job import MRJob

class ReplaceAuthorNumber(MRJob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.authors = {}
        with open('Auteurs.txt', 'r') as f:
            next(f)  # skip the header
            for line in f:
                author_number, author_name = line.strip().split(',')
                self.authors[author_number] = author_name

    def mapper(self, _, line):
        book_number, book_title, author_number = line.split(',')
        yield(book_number, (book_title, self.authors.get(author_number)))

    def reducer(self, key, values):
        for value in values:
            book_title, author_name = value
            yield(key, (book_title, author_name))

if __name__ == '__main__':
    ReplaceAuthorNumber.run()