#!/usr/bin/python3
import sys
import configparser
import argparse
import logging
import time
from pprint import pprint
from mng import MyProc

def myParser():
    parser = argparse.ArgumentParser(description='Simple replication of mongo documents.')
    parser.add_argument('-c', '--cfg', help="configuration file, default is replicator.cfg", default="replicator.cfg")
    args = parser.parse_args()
    return args 

def srcDst(config):
    out = {}
    locations= ["source", "destination"]
    params = ["host", "login", "database", "password", "keyword", "seconds",
        "collections", "seconds", "remap", "keepid"]
    for loc in locations:
        p = {}
        for param in params:
            if param in config[loc].keys():
                if "," not in config[loc][param]:
                    p[param] = config[loc][param]
                else:
                    p[param] = config[loc][param].split(",")
        out[loc] = p
    return out

def main():
    logging.basicConfig(filename='./replicator.log', encoding='utf-8',
                        format='%(asctime)s|replicator.py|%(message)s', 
                        level=logging.DEBUG)
    logging.info("Starting %s" % (sys.argv[0]))
    opts = myParser()
    cfg = configparser.ConfigParser()
    cfg.read(opts.cfg)
    sd = srcDst(cfg)
    pprint(sd)
    workClass = MyProc(sd)
    
    while(True):
        workClass.refresh()
        time.sleep(int(sd["source"]["seconds"])*1000)

if __name__ == "__main__":
    main()
