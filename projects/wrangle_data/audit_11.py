# -*- coding: utf-8 -*-
"""
Created on Sun Jun 19 14:22:24 2016

@author: andrew

Edit: Jan 8 2017

Fixes syntax error that omitted 2nd level tags "k"

note pprint option set to "False" on "process map"

Udacity Project 3
 Data Audit of Test osm file
 
Adapts code from L6
    audit
    MapParser
    Tags
    Users
    Data
    
Below: data object "streets": type : dict # counts the questionable street names
 
 Tag Analysis Categories
 
  "lower", for tags that contain only lowercase letters and are valid,
  "lower_colon", for otherwise valid tags with a colon in their names,
  "problemchars", for tags with problematic characters, and
  "other", for other tags that do not fall into the other three categories.

"""

import sys
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import logging

import codecs
import json


# puts log file in overwrite mode
# filemode="w"

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='sample.log',
                    filemode='w')

#testing with sample file
OSMFILE = "mng_wpb_pmp.osm"

# following is the full size 50 MB file
#"mng_wpb_pmp.osm"

# re matches very last word
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
addr_start = re.compile(r'^(addr):([a-z]|_)*$')
double_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Circle", "Highway"]

# dict of common street abreviations
mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Rd": "Road",
            "Rd.": "Road",
            "Trl": "Trail",
            "Trl.": "Trail",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "Hwy": "Highway",
            "Hwy.": "Highway",
            "Ct": "Court",
            "Ct.": "Court"
            }


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

def update_name(name, mapping):

    searchGrouped = ""
    searchObj = re.search( street_type_re, name)
    
    if searchObj:
        searchGrouped = searchObj.group()
        num = len(searchGrouped)
         
    if searchGrouped in mapping:
        name = name[:-num] + mapping[searchGrouped]
        
    return name
    
def count_tags(filename):
	tags = {}

	
	for a in ET.iterparse(filename):
		if a[1].tag in tags:
			# increment
			tags[a[1].tag] = tags[a[1].tag] +1
		else:
			#add new dict key
			tags[a[1].tag] = 1

	return tags    


lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    
    if element.tag == "tag":
        # YOUR CODE HERE
        for tag in element.iter("tag"):
            
            matchLow = re.search(lower, tag.attrib['k'])
            if matchLow:
                # print matchLow.group()
                keys["lower"] = keys["lower"]+1
            
            matchColon = re.search(lower_colon, tag.attrib['k'])
            if matchColon:
                # print matchColon.group()
                keys["lower_colon"] = keys["lower_colon"]+1
            
            matchProb = re.search(problemchars, tag.attrib['k'])
            if matchProb:
                # print matchProb.group()
                keys["problemchars"] = keys["problemchars"]+1
            
            if (matchLow == None and matchColon == None and matchProb == None):
                # print tag.attrib['k']
                keys["other"] = keys["other"] +1            
      
    return keys


def key_audit(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys    

def get_users(filename):
    users = set()
    for _, element in ET.iterparse(filename):

        #node
        if element.tag == "node":
            users.add(element.attrib['uid'])
        #way
        if element.tag == "way":
            users.add(element.attrib['uid'])
        #relation
        if element.tag == "relation":
            users.add(element.attrib['uid'])

    return users

# from data.py

def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data
    
def shape_element(element):
    node = {}

    if element.tag == "node" or element.tag == "way" :
        # YOUR CODE HERE
        
        exclude = []
        node['address'] = {}
        node['created'] = {}
        node['type'] = element.tag
  
        # first level: attrib in the opening tag
        for key in element.attrib:

            if key in CREATED:
                node['created'] [key] = element.attrib[key]

            if key == 'lon':
                node['pos'] = [0,0]
                node['pos'][1] = ( float(element.attrib[key]) )
            if key == 'lat':
                node['pos'][0] = ( float(element.attrib[key]) )                

            if (key not in CREATED) and (key != 'ref') and (key != 'lat') and (key != 'lon') and (key not in exclude):
                node[key] = element.attrib[key]

        #for second level
        if element.tag == "way":
            node['node_refs'] = []
            for nd in element.iter("nd"):
                node['node_refs'].append( nd.attrib['ref'] )

    
        #for second level
        for tag in element.iter("tag"):
            
            matchProb = re.search(problemchars, tag.attrib['k'])
            if matchProb:
                #print "gotProb"
                exclude.append(tag.attrib['k'])
                
            matchDoubleColon = re.search(double_colon, tag.attrib['k'])
            if matchDoubleColon:
                # evaluated thrice
                #print "gotDoubleColon"
                exclude.append(tag.attrib['k'])

            matchAddr = re.search(addr_start, tag.attrib['k'])
            if matchAddr:
                #evaluated
                #print "gotAddr_Match"
                # check for name correction
                better_name = update_name((tag.attrib['v']) , mapping)
                                # slice off addr: [0:4] take the rest of it 
                                # and append to address sub - dict
                node['address'] [tag.attrib['k'][5:]] = better_name

            # all other second level tag data
            node[tag.attrib['k']] = tag.attrib['v']        

        #del empty address dict
        if not(bool(node['address']) ):
            node.pop('address', None)

                
        return node
        
    else:
        return None


def test():
    """
    Note: this version writes output to log file
    """
    streets = audit(OSMFILE)
    
    logFile = open('audit_11'+'.log', 'w')

    #output from audit

    x = len(streets)
    pprint.pprint( '' , logFile )
    pprint.pprint( x, logFile)
    pprint.pprint(  " questionable street names" , logFile )
    pprint.pprint( '' , logFile )   

    pprint.pprint( '' , logFile )
    pprint.pprint( "Bad Data: Street Names", logFile)
    pprint.pprint( '' , logFile )

    pprint.pprint(dict(streets), logFile, indent = 2, width = 5 )




    pprint.pprint( '' , logFile )
    pprint.pprint("Bad Data: Street Name Change suggestions", logFile)
    pprint.pprint( '' , logFile )

    for streets, ways in streets.iteritems():
        for name in ways:
            better_name = update_name(name, mapping)
            line_out = name, "=>" , better_name
            pprint.pprint(line_out, logFile)

    # tag survey count
    tags = count_tags(OSMFILE)
    
    pprint.pprint( '' , logFile )
    pprint.pprint("tag survey count", logFile)
    pprint.pprint( '' , logFile )
    pprint.pprint(tags, logFile, indent = 2, width = 5)

    # tags: key audit

    keys = key_audit(OSMFILE)

    pprint.pprint( '' , logFile )
    pprint.pprint("keys audit count", logFile)
    pprint.pprint( '' , logFile )
    pprint.pprint(keys, logFile, indent = 2, width = 5)

    # find number of contributing users

    users = get_users(OSMFILE)
    
    y = len(users)
    
    pprint.pprint( '' , logFile )
    pprint.pprint( y, logFile)
    pprint.pprint(  " unique users" , logFile )
    pprint.pprint( '' , logFile )

    pprint.pprint( '' , logFile )
    pprint.pprint("users audit: user id numbers", logFile)
    pprint.pprint( 'omitted' , logFile )
    # the following could be a long list
    #pprint.pprint(users, logFile, indent = 2, width = 5)
    
    # from data.py
    # processes data into a list of dictionaries of mapping data
    data = process_map(OSMFILE, False)
    
    # output to log file
    pprint.pprint( '' , logFile )
    pprint.pprint(data, logFile)    
    pprint.pprint( '' , logFile )
    
    pprint.pprint( 'End of Audit' , logFile )
    
    logFile.close()  

    return None

# runs test when this file is called
if __name__ == '__main__':
    test()
