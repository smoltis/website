#!/usr/bin/env python
# encoding: utf-8
"""
korsimport.py

Created by Stan Smoltis on 2016-10-24.
Copyright (c) 2016 Stan Smoltis. All rights reserved.
"""
from dbfread import DBF
from pandas import DataFrame
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as bb
from time import time


class RecordPart(object):
    def __init__(self, items):
        for (name, value) in items:
            #if name in ['C_TOV', 'ART', 'TOV_SNAME', 'TOV_CENR1', 'TOV_UPAK', 'TOV_ED', 'PROIZV', 'IZGOT']:
            setattr(self, name, value)


#class RecordInv(object):
#    def __init__(self, items):
#        for (name, value) in items:
            #if name in ['C_TOV', 'TOV_KOL', 'TOV_REZ']:
#            setattr(self, name, value)



class DBFImporter(object):

    def __init__(self, index, type_name):
        self.parts_dict = None
        self.inventory_dict = None
        self.index_name = index
        self.type_name = type_name

    def load_files(self,file_dict):
        t0 = time()
        for key,value in file_dict.items():
            if key == 'parts':
                self.parts_dict = DBF(value, encoding="1251", ignore_missing_memofile=True,
                                      ignorecase=False, load=True, recfactory=RecordPart,lowernames=True)
            elif key == 'inventory':
                self.inventory_dict = DBF(value, encoding="1251", ignore_missing_memofile=True,
                                          ignorecase=False, load=True)
        print("done in %.3fs" % (time() - t0))

    def recreate_es_index(self,es_connection):
        t0 = time()

        if es_connection.indices.exists(self.index_name):
            print("deleting '%s' index..." % (self.index_name))
            res = es_connection.indices.delete(index=self.index_name)
            print(" response: '%s'" % (res))

        request_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }

        print("creating '%s' index..." % (self.index_name))
        res = es_connection.indices.create(index=self.index_name, body=request_body)
        print(" response: '%s'" % (res))
        print("done in %.3fs" % (time() - t0))

    def create_dataset(self):
        t0 = time()

        docs = list()
        #columns = ['C_TOV', 'ART', 'TOV_SNAME', 'TOV_CENR1', 'TOV_UPAK', 'TOV_ED', 'PROIZV', 'IZGOT']
        #inv_col = ['C_TOV', 'TOV_KOL', 'TOV_REZ']
        #grby_col = columns[0]

        #l=list()
        #for record in self.inventory_dict:
        #        print("{0}, {1}, {2}".format(record.c_tov,record.tov_kol,record.tov_rez))


        inventory_df = DataFrame(list(self.inventory_dict))

        #inventory_df = df.groupby(['C_TOV'], axis=1).sum() #apply groupby and take sum()

        for part in self.parts_dict:

            prod_id = part.c_tov

            ser = inventory_df.loc[inventory_df['C_TOV'] == prod_id]

            if not ser.empty:
                qty = ser['TOV_KOL'].sum()
                qty_res = ser['TOV_REZ'].sum()
            else:
                qty=0
                qty_res=0

            data = {
                '_id':prod_id,
                'ProductID': prod_id,
                'PartNo': part.art,
                'PartName': part.tov_sname,
                'Price': part.tov_cenr1,
                'Package': part.tov_upak,
                'Pct': part.tov_ed,
                'Country': part.izgot,
                'Manufacturer': part.proizv,
                'Qty': qty,
                'Qtyres': qty_res
            }
            docs.append(data)
        print("done in %.3fs" % (time() - t0))
        return docs

    def es_bulk_index(self, es_connection, data):
        t0 = time()

        try:
            # es.bulk(body=docs,index=INDEX_NAME,doc_type=TYPE_NAME)
            bb(client=es_connection, actions=data, index=self.index_name, doc_type=self.type_name)
        except UnicodeDecodeError as e:
            print("bulk load error! " + e)

        print("done in %.3fs" % (time() - t0))

    def es_sanity_check(self,es_connection):
        try:
            res = es_connection.search(index=self.index_name, size=2, body={"query": {"match_all": {}}})
            print(" response: '%s'" % (res))
        except UnicodeDecodeError as e:
            print("Decode error ...")
            print(e)


############################################################################################################
if __name__ == '__main__':

    files = {
                'parts': '/home/stas/Documents/WORK_PROJECTS/Wolf_KORS/kors/Baza/tovar.dbf',
                'inventory': '/home/stas/Documents/WORK_PROJECTS/Wolf_KORS/kors/Baza/skladt.dbf'
    }
    ES_HOST = {
                'host': '127.0.0.1',
                'port': 9200
    }
    INDEX_NAME = 'kors_py'
    TYPE_NAME = 'part'

    #create ES connection
    es_conn = Elasticsearch()
    #['127.0.0.1'],
                            #http_auth=('user', 'secret'),
     #                       maxsize=25,
     #                       sniff_on_start=True,
     #                       sniff_on_connection_fail=True,
     #                       sniffer_timeout=60)

    # create an instance of a class
    cl = DBFImporter(INDEX_NAME, TYPE_NAME)

    # load the files
    cl.load_files(files)

    # create list of documents to load
    actions = cl.create_dataset()
    #print(actions.count)

    # create an empty index
    cl.recreate_es_index(es_conn)

    # bulk load data to ES
    cl.es_bulk_index(es_conn,actions)

    # perform index check in ES
    cl.es_sanity_check(es_conn)




############################################################################################################
