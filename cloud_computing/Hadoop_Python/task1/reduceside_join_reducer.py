#!/usr/bin/python3

import sys

def read_map_output(file):
    """ Return an key-value pair extracted from file (sys.stdin).
    Input format: key \t value
    Output format: (key, value)
    """
    for line in file:
        yield line.strip().split('\t', 1)


def combine_place():
    """ This reducer run reduce side join
    Input format: place_id#0 \t place_url \t place_type_id
                  place_id#1 \t tags
    Ourput format: place_url \t place_type_id \t tags
    """

    data = read_map_output(sys.stdin)

    current_place_id = ''
    current_place_url_and_type_id= 'NULL'

    #Check there is more than 2 value in record
    for record in data: #for key, value in data:
        if len(record) != 2:
            continue
        key,value = record[0], record[1]
        # check input is valid
        if key == '':
            continue

        # split key by '#' , get the place_id and a number
        key = key.split('#')

        # check the key-value pair come from place or come from photo
        if key[0] != current_place_id:
            if key[1] == '0':
                current_place_id = key[0]
                current_place_url_and_type_id = value
            else:
                current_place_id = key[0]
                current_place_url_and_type_id = 'Place_not_found'

                print(current_place_url_and_type_id + '\t' + value)
        else:
            print(current_place_url_and_type_id + '\t' + value)

    

if __name__ == '__main__':
    combine_place()