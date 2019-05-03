import sys
from configparser import ConfigParser

from .clearinghouse_repository import ClearinghouseRepository
from .html_parser import HTMLFile


class PemsExtractor:

    def __init__(self, config_file):
        self._parse_config(config_file)
        self.links = []
        self.repo = ClearinghouseRepository(self.authorization)

    def _parse_config(self, config_path):

        config = ConfigParser()
        config.read(config_path)

        self.html_path = config.get('Paths', 'html_file_path')
        self.log_path = config.get('Paths', 'log_file_path')
        self.out_path = config.get('Paths', 'out_dir_path')

        username = config.get('Creds', 'username')
        pwd = config.get('Creds', 'password')

        self.authorization = {
            'username': username,
            'password': pwd
        }

    def extract_links(self):
        html_file = HTMLFile(self.html_path)

        return html_file.create_links()

    def get_files(self, links=None):

        if links is None:
            links = self.links

        self.repo.download_files(links, self.out_path)
        print('download finished')


if __name__  == "__main__":
    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    config_path = sys.argv[1]

    pems = PemsExtractor(config_path)
    links = pems.extract_links()
    print("extracted Links, found: ", len(links))

    pems.get_files(links)
