import sys
sys.path.append('./src')

from pems_extract.pems_extractor import PemsExtractor
from utils import locate_config


if __name__ == "__main__":

    config_path = locate_config(sys.argv)

    pems = PemsExtractor(config_path)
    links = pems.extract_links()
    print("extracted Links, found: ", len(links))

    pems.get_files(links)