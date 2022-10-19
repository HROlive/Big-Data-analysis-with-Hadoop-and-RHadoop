from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    """
    A class to represent a Word Frequency Count mapreduce job
    """
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
