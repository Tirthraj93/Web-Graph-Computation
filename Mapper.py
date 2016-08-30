from os.path import join
import os
from Constants import *
from django.utils.encoding import iri_to_uri


class Mapper(object):
    """
    Common class for the team, to map crawled web graph links for optimization
    """
    prevID = 0

    def __init__(self, mappings={}):
        self.mappings = mappings

    @classmethod
    def fromFile(self, name, reverse=False):
        print "Getting the mapper ready!"
        linkMapPath = construct_path(name)
        link_map = {}
        with open(linkMapPath, 'r') as m:
            lines = m.readlines()
            count = 1
            for line in lines:
                split_line = line.split('\t')
                key, value = Mapper.getKeyValue(split_line, reverse)
                link_map[key] = value
                print count
                count += 1

        return self(link_map)

    @staticmethod
    def getKeyValue(split_line, reverse):
        """
           Returns a list [key, value] with key and values based on the reverse
           value
       """
        ID = split_line[1].strip()
        link = split_line[0]

        if reverse:
            return (ID, link)

        return (link, ID)

    def map(self, link):
        if link in self.mappings:
            return self.mappings.get(link)
        else:
            self.prevID += 1
            self.mappings[link] = self.prevID
            return self.prevID

    def lookup(self, link):
        if link in self.mappings:
            return self.mappings.get(link)
        else:
            raise ValueError('The token could not be found in the tokenmap!')

    def write(self, path):
        print "Writing Doc ID Mappings to file"
        self.__write_to_file('link_map.txt', self.mappings, path)

    def __write_to_file(self, name, data, folder):
        print "Writing to file..."
        folder_path = join(MAPPINGS_PATH, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        path = join(folder_path, name)
        with open(path, 'w') as m:
            print '\twriting ', len(self.mappings)
            for link, ID in self.mappings.iteritems():
                try:
                    link = iri_to_uri(link)
                    string = "{}\t{}\n".format(link, ID)
                    m.write(string)
                except Exception as e:
                    print 'error'


    def __str__(self):
        return str(self.mappings)


def construct_path(name):
    # folder_path = join(mappings_path, folder)
    return join(MAPPINGS_PATH, name)


if __name__ == '__main__':
    mapper = Mapper.fromFile('link_map.txt')
    print mapper
