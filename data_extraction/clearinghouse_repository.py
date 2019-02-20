# TODO: add in logging for files
# TODO: add in way to download html file
# TODO: finish error handling
# import logging
from connector import Connector


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

    def download_html_file(self):
        print("not yet implmemented")
        pass

    def download_files(self, links, out_path):
        """
        Function downloads files from Caltrans 1 by 1
        :param links: links to the files
        :param out_path: the path that the files should be saved at
        """
        conn = self.connection.start_connection()

        for i, link in enumerate(links):

            try:
                self.download_file(conn, link, out_path)
            except ConnectionError("Error in connection"):
                conn = self.connection.start_connection()
                continue
            except:
                print("error in downloading, something else. will continue?")




