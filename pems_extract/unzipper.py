import zipfile


def unzip_file(zip_path, extract_path):

    zip_ref = zipfile.ZipFile(zip_path, 'r')
    zip_ref.extractall(extract_path)
    zip_ref.close()