#!/usr/bin/python3
# coding: utf-8
import sys


def tag_mapper():
    """ This mapper read the input from multiple reducer files
    Input format:   place_url_1 \t {tag_n = count}
                    place_url_1 \t {tag_n = count}
                    place_url_2 \t {tag_n = count}
                    place_url_2 \t {tag_n = count}
    Output format:  place_url_1 \t {tag_n = count}
                    place_url_1 \t {tag_n = count}
    """

    # Test input form reducer multiple file
    for line in sys.stdin:
        items = line.strip().split('\t')
        place_url, tags = items[0].strip(), items[1].strip()
        print(place_url + '\t' + tags)



if __name__ == '__main__':
    tag_mapper()