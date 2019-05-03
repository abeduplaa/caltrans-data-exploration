import sys
from data_extraction.pems_extractor import PemsExtractor


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(len(sys.argv))
        raise TypeError("ERROR: need to provide path to config file.")

    config_path = sys.argv[1]

    pems = PemsExtractor(config_path)
    links = pems.extract_links()
    print("extracted Links, found: ", len(links))

    pems.get_files(links)
