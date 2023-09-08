from mrjob.job import MRJob

class ReplaceAuthorNumber(MRJob):
    def configure_args(self):
        super(ReplaceAuthorNumber, self).configure_args()
        self.add_file_arg('--authors')

    def mapper_init(self):
        self.authors = {}
        with open(self.options.authors, 'r') as f:
            for line in f:
                author_number, author_name = line.strip().split(',')
                self.authors[author_number] = author_name

    def mapper(self, _, line):
        book_number, book_title, author_number = line.split(',')
        yield(book_number, (book_title, author_number))

    def reducer(self, key, values):
        for value in values:
            book_title, author_number = value
            author_name = self.authors.get(author_number)
            yield(key, (book_title, author_name))

if __name__ == '__main__':
    ReplaceAuthorNumber.run()