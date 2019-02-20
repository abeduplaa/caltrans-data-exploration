from bs4 import BeautifulSoup


class HTMLFile:

    def __init__(self, html_path, base_url='http://pems.dot.ca.gov'):
        with open(html_path) as fp:
            self._soup = BeautifulSoup(fp, features="html.parser")
        self.base_url = base_url

    def create_links(self):
        links = []
        for a in self._soup.find_all('a'):
            download_section = a.get('href')
            url = self.base_url + download_section
            links.append(url)

        return links

