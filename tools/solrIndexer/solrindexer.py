#!/usr/bin/env python
# coding: utf-8

import logging
import requests
import json
import sqlite3
import os
import json
import tqdm
import hashlib
import pdftotext
import sys
from datetime import datetime
import dateutil.parser

## Solr get Requests
def solr_get(url, params):
    log = logging.getLogger('calibre_indexer')
    r = requests.get(url, params=params)
    json = r.json()
    if not r.status_code == 200 or not json["responseHeader"]["status"] == 0:
        log.error("error getting from Solr: {}".format(json))
        return {}
    return json

## Solr post Requests
def solr_post(url, params, data):
    log = logging.getLogger('calibre_indexer')
    r = requests.post(url, params=params, json=data, headers={"Content-Type": "application/json"})
    json = r.json()
    if not r.status_code == 200 or not json["responseHeader"]["status"] == 0:
        log.error("error posting to Solr: {}".format(json))
        return {}
    return json

## create MD5 Hash from file
def md5(file):
# TODO Error handling / not enough memory
    md5_hash = hashlib.md5()
    f = open(file, "rb")
    content = f.read()
    md5_hash.update(content)
    return md5_hash.hexdigest()

## check if book ID is in calibre DB
def is_in_calibre(book_id, con_string):
    con = sqlite3.connect(con_string, uri=True)
    cursor = con.cursor()
    return True if cursor.execute("SELECT id FROM books WHERE id = {}".format(book_id)) else False

## Read and add book Information to book from Calibre DB
def read_calibre(book, con_string):
    log = logging.getLogger('calibre_indexer')
    try:
        con = sqlite3.connect(con_string, uri=True)
        cursor = con.cursor()
        log.debug("Successfully Connected to CalibreDB")

        book["path"] = [x[0] for x in cursor.execute("SELECT path FROM books " +
                "WHERE id = {} ".format(book["id"])).fetchall()][0]
        book["published"] = [dateutil.parser.isoparse(x[0]).date().isoformat()
                for x in cursor.execute("SELECT pubdate FROM books " +
                "WHERE id = {} ".format(book["id"])).fetchall()][0]

        paths = ["/{}.pdf".format(x[0]) for x in cursor.execute("SELECT name FROM data WHERE " + 
                "book = {} AND format = '{}'".format(book["id"], "PDF"))]
        if paths:
            book["path"] += paths[0]
        else:
            book["path"] = ''

        lang_codes = [x[0] for x in cursor.execute("SELECT languages.lang_code FROM books_languages_link " +
                "LEFT JOIN languages ON books_languages_link.lang_code = languages.id " +
                "WHERE books_languages_link.book = {}".format(book["id"])).fetchall()]
        if lang_codes:
            book["lang_code"] = lang_codes[0]
        else:
            book["lang_code"] = "ge"

        book["authors"] = [x[0] for x in cursor.execute("SELECT authors.name FROM books_authors_link " +
                "LEFT JOIN authors ON books_authors_link.author = authors.id " +
                "WHERE books_authors_link.book = {}".format(book["id"])).fetchall()]

        book["publishers"] = [x[0] for x in cursor.execute("SELECT publishers.name FROM books_publishers_link " +
                "LEFT JOIN publishers ON books_publishers_link.book = publishers.id " +
                "WHERE books_publishers_link.book = {}".format(book["id"])).fetchall()]

        book["title_{}".format(book["lang_code"])] = [x[0] for x in cursor.execute("SELECT title FROM books " +
                "WHERE id = {}".format(book["id"])).fetchall()][0]

        book["tags_{}".format(book["lang_code"])] = [x[0] for x in cursor.execute("SELECT tags.name FROM books_tags_link " + 
                "LEFT JOIN tags ON books_tags_link.tag = tags.id " +
                "WHERE books_tags_link.book = {}".format(book["id"])).fetchall()]

        del_keys = []
        for key in book.keys():
            if not book[key]: del_keys.append(key)
        for key in del_keys:
            del book[key]

        cursor.close()
    except sqlite3.Error as error:
        log.error("Error while connecting to sqlite: {}".format(error))
    finally:
        if (con):
            con.close()
            log.debug("The SQLite connection is closed")

## Read text from pdf and add pages to book
def read_pdf(book, lang_code, path_to_file):
    log = logging.getLogger('calibre_indexer')
    if not os.path.isfile(path_to_file): return
    file = open(path_to_file, 'rb')
    try:
        pdf = pdftotext.PDF(file)
        for page in tqdm.tqdm(range(len(pdf))):
            try:
                book["page_{}_{}".format(lang_code, page + 1)] = pdf[page]
            except pdftotext.Error as err:
                log.error(err)
                continue
    except pdftotext.Error as err:
        log.error(err)
    finally:
        file.close()

## Deletes all documents from Solr DB
def clear_solr_db(solr_core):
    print("deleting all documents form Solr")
    post = solr_post(solr_core + "/update", {"commit": "true"}, {"delete": { "query": "*:*" }})
    if post:
        print("deleted all docs from Solr")
    else:
        print("something went wrong ...")

## worker gathers book data for book ID, compares it with current solr state and 
## sends data to solr indexer
def worker(book_id, con_string, solr_core, calibre_path):
    log = logging.getLogger('calibre_indexer')
    book = {"id": book_id}

    ## Check if book ID is in Calibre DB. If not delete from Solr
    if not is_in_calibre(book_id, con_string):
        log.info("Deleting bookID {} from SOLR".format(book["id"]))
        solr_post(solr_core + "/update", {"commit": "true"}, {"delete": { "id": book["id"] }})
    else:
    ## Read book data from Calibre DB
        read_calibre(book, con_string)
        log.info({"id": book["id"], "title": book["title_" + book["lang_code"]]})

    ## Read book data from Solr DB
        solr_data = solr_get(solr_core + "/select", {"q": 'id:"{}"'.format(book["id"]),
                "fl": "id,title_deu,title_eng,title_ge,tags_deu,tags_eng,tags_ge," +
                    "lang_code,authors,md5,publishers,published"})
        solr_book = {}
        if solr_data and solr_data["response"]["docs"]: 
            solr_book = solr_data["response"]["docs"][0]
            solr_book["published"] = dateutil.parser.isoparse(solr_book["published"]).date().isoformat()
        
    ## book_path := Path to pdf file
        book_path = ''
        if 'path' in book.keys(): 
            book_path = os.path.join(calibre_path, book["path"])
            del book["path"]
        if not book_path or not os.path.isfile(book_path):
            log.info("BookID {} has no pdf file".format(book["id"]))
        else:
            book["md5"] = md5(book_path)

    ## Compare book Data from Calibre and Solr
    ## If data doesnt match pdf pages are added to book data and send to Solr Indexer
        if solr_book == book:
            log.info("no changes --> skipping")
        else:
            log.info("changes detected" if solr_book else "new book")
            if book_path: read_pdf(book, book["lang_code"], book_path)
            post = solr_post(solr_core + "/update/json/docs", {"commit": "true"}, book)
            log.info("success" if post else "Something went wrong...")

## Costum tqdm compatible handler for logging
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

def main(calibre_path, solr_core, log_level):
    ## Set logger with tqdm compatible handler
    log = logging.getLogger('calibre_indexer')
    log.setLevel(logging.getLevelName(log_level))
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler = TqdmLoggingHandler()
    log_handler.setFormatter(formatter)
    log.addHandler (log_handler)

    ## connection String for Sqlite3 Calibre DB
    con_string = "file:{}?mode=ro".format(os.path.join(calibre_path, "metadata.db"))

    calibre_books = []
    solr_books = []
    ## Read IDs of books in Calibre DB
    try:
        con = sqlite3.connect(con_string, uri=True)
        cursor = con.cursor()
        log.info("Successfully Connected to CalibreDB")
        calibre_books = [str(x[0]) for x in cursor.execute("SELECT id FROM books " +
                "ORDER BY id ASC").fetchall()]
        cursor.close()
    except sqlite3.Error as error:
        log.error("Error while connecting to sqlite: {}".format(error))
    finally:
        if (con):
            con.close()
            log.debug("The SQLite connection is closed")

    ## Read IDs of Books in Solr DB
    solr_books_num = 1
    while (len(solr_books) < solr_books_num):
        solr_data = solr_get(solr_core + "/select", 
            {"q": "*", "fl": "id", "rows": 100, "start": len(solr_books)})
        if not solr_books: solr_books_num = solr_data["response"]["numFound"]
        solr_books += [x["id"] for x in solr_data["response"]["docs"]]
    
    ## Generate a List of unique IDs with sorting 
    ## 1. Books that can be deleted from Solr DB
    ## 2. Books that musst be added to Solr DB
    ## 3. Books that possibly need update
    in_calibre = set(calibre_books)
    in_solr = set(solr_books)
    in_solr_but_not_calibre = in_solr - in_calibre
    in_calibre_but_not_solr = in_calibre - in_solr
    in_both = list(in_calibre - in_calibre_but_not_solr)
    books = list(in_solr_but_not_calibre) + list(in_calibre_but_not_solr) + in_both

    ## Iterate over List of Book IDs and send to worker
    for bookID in tqdm.tqdm(books):
        worker(bookID, con_string, solr_core, calibre_path)

if __name__ == "__main__":
    ## define and read arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--delete', help='Deletes all documents from Solr DB', action="store_true")
    parser.add_argument('solr', type=str, help='Solr URL to dedicated Core')
    parser.add_argument('-c', '--calibre', type=str, help='Path to Calibre folder')
    parser.add_argument('-l', '--level', type=str, help='Debug level - Default: INFO', default='INFO')
    args = parser.parse_args()

    if not args.calibre:
        print("No Calibre folder provided!")

    ## Clear Solr DB
    if args.delete:
        clear_solr_db(args.solr)
    
    ## Start indexing
    if args.calibre:
        main(args.calibre, args.solr, args.level)