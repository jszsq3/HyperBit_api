# coding=utf-8
import os
import re
import ctypes
import json
import codecs

so = ctypes.cdll.LoadLibrary
print(os.getcwd() +"/libtriplebitApi.so")
mylib = so(os.getcwd() +"/libtriplebitApi.so")
mylib.search.restype = ctypes.c_char_p
mylib.search_hyper.restype = ctypes.c_char_p
mylib.insert.restype = ctypes.c_int
# mylib.search.argtypes = [ctypes.c_char_p,ctypes.c_char_p]
def Select(database,query):
    querypath = '../q1'
    with open(querypath,'w+') as f:
        f.write(query)
    
    res1 = mylib.search(ctypes.create_string_buffer(database.encode('utf-8')),ctypes.create_string_buffer(querypath.encode('utf-8')))
    return res1

def Select_hyper(database,query):
    querypath = '../q1'
    with open(querypath,'w+') as f:
        f.write(query)
    
    res1 = mylib.search_hyper(ctypes.create_string_buffer(database.encode('utf-8')),ctypes.create_string_buffer(querypath.encode('utf-8')))
    return res1

def Insert(database,inserpath):
    res = mylib.insert(ctypes.create_string_buffer(database.encode('utf-8')),ctypes.create_string_buffer(inserpath.encode('utf-8')))
    return res

def query_gene(headEntity, relation, tailEntity):
    selStr = ''
    queryType = 0
    if headEntity == '':
        headEntity = '?x'
        selStr += headEntity + ' '
        queryType += 1
    else:
        headEntity = "<" + headEntity + ">"
    if relation == '':
        relation = '?y'
        selStr += relation + ' '
        queryType += 2
    else:
        relation = "<" + relation + ">"
        
    if tailEntity == '':
        tailEntity = '?z'
        selStr += tailEntity + ' '
        queryType += 4
    else:
        tailEntity = "<" + tailEntity + ">"
    query = "select %s where { %s %s %s .}" % (selStr, headEntity, relation, tailEntity)
    print(query)
    return query
# print(Select('../database/','select ?x where{ ?x <社会信用代码>  <92350206MA2Y3F7E8R> .}'))

def query_gene_hyper(subject ,predicate ,object):
    project_str = ''
    if subject!='':
        subject_str = ' ?h <rdfs> <'+subject+'> .\n'
        if predicate!='':
            predicate_str = ' ?h <rdfp> <'+predicate+'> .\n'
            if object!='':
                object_str = ' ?h <rdfo> <'+object+'> .\n'
            elif object=='':
                object_str = ''
        elif predicate=='':
            predicate_str = ''
            if object!='':
                object_str = ' ?h <rdfo> <'+object+'> .\n'
            elif object=='':
                object_str = ''
    elif subject=='':
        subject_str = ''
        if predicate!='':
            predicate_str = ' ?h <rdfp> <'+predicate+'> .\n'
            if object!='':
                object_str = ' ?h <rdfo> <'+object+'> .\n'
            elif object=='':
                object_str = ''
        elif predicate=='':
            predicate_str = ''
            if object!='':
                object_str = ' ?h <rdfo> <'+object+'> .\n'
            elif object=='':
                object_str = ''

    query = "select ?h ?x ?y ?z ?k ?m where {\n%s%s%s ?h <rdfs> ?x .\n ?h <rdfp> ?y .\n ?h <rdfo> ?z .\n ?h <rdfst> ?k .\n ?h <rdfet> ?m .\n }" %( subject_str , predicate_str , object_str)
    return query