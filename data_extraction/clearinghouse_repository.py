# TODO: add in logging for files
# TODO: add in way to download html file
# TODO: finish error handling
# import logging
import gzip
import os
from .connector import Connector


def gunzip(source_filepath, dest_filepath, block_size=65536):
    with gzip.open(source_filepath, 'rb') as s_file, \
            open(dest_filepath, 'wb') as d_file:
        while True:
            block = s_file.read(block_size)
            if not block:
                break
            else:
                d_file.write(block)
        d_file.write(block)

class ClearinghouseRepository:
    def __init__(self, authorization):
        self.connection = Connector(authorization)

    @staticmethod
    def download_file(conn, link, out_path):
        raw = conn.get(link)
        file_name = raw.headers['content-disposition'].split('=')[-1]
        with open(out_path + file_name, 'wb') as fi:
            fi.write(raw.content)

        print("Written File: ", file_name)

        return out_path+file_name


    def download_html_file(self):
        print("not yet implmemented")
        pass

    def extract_files(self, in_path):
        out_path = os.path.splitext(in_path)[0]
        gunzip(in_path, out_path)
        os.remove(in_path)

    def download_files(self, links, out_path):
        """
        Function downloads files from Caltrans 1 by 1
        :param links: links to the files
        :param out_path: the path that the files should be saved at
        """
        conn = self.connection.start_connection()

        for i, link in enumerate(links):

            try:
                filepath = self.download_file(conn, link, out_path)
                self.extract_files(filepath)

            except ConnectionError("Error in connection"):
                conn = self.connection.start_connection()
                continue
            except:
                print("error in downloading, something else. will continue?")






