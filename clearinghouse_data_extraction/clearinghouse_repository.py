# TODO: add in logging for files
# TODO: add in way to download html file
# TODO: finish error handling
# import logging
from connector import Connector


class ClearinghouseRepository:
    def __init__(self, authorization):
        self.connection = Connector(authorization)

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

                    print(link)
                    r = conn.get(link)
                    print('finished download to local var')
                    fname = r.headers['content-disposition'].split('=')[-1]

                    with open(out_path + fname, 'wb') as fi:
                        fi.write(r.content)

                    print("Written File: ", fname)
                except ConnectionError:
                    print("error in downloading, something wrong with connection")
                    conn = self.connection.start_connection()
                    continue
                except:
                    print("error in downloading, something else. will continue?")




