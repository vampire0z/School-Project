#!/usr/bin/python3

import sys


def multi_mapper():
    """ This mapper will output different format dependind on input type
    If input is place file:
    Input format: place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
    Output format: place_id#0 \t place_url \t place_type_id
                
    If input is photo file:
    Input format: photo_id \t owner \t tags \t date_taken \t place_id \t accuracy
    Output format: place_id#1 \t tags
    """
    for line in sys.stdin:
        parts = line.strip().split("\t")
        
        if len(parts) == 7:
            place_id, place_type_id, place_url = parts[0].strip(), parts[5].strip(), parts[6].strip()
            if place_type_id == '7' or place_type_id == '22':
                print(place_id + "#0\t" + place_url + "\t" + place_type_id)
        
        elif len(parts) == 6:
            photo_id, tags, place_id = parts[0].strip(), parts[2].strip(), parts[4].strip()
            #print(place_id + "#1\t" + photo_id + "\t" + tags)
            print(place_id + "#1\t" + tags)


if __name__ == "__main__":
    multi_mapper()