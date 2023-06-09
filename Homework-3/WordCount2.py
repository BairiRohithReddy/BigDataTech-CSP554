from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            if re.match(r'^[a-nA-N]\w*', word):
                yield 'a-n', 1
            else:
                yield 'other', 1

    def combiner(self, category, counts):
        yield category, sum(counts)

    def reducer(self, category, counts):
        yield category, sum(counts)

if __name__ == '__main__':
    MRWordCount.run()
