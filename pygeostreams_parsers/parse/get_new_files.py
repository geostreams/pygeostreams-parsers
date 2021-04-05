"""
    Get New Files for a Defined Location
"""


import os
import yaml
import argparse
import requests
from pyclowder.datasets import get_file_list


def main():
    """Main Function"""

    # Required command line argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    multi_config = yaml.load(open(opts.config, 'r'))

    url = multi_config['inputs']['location']
    key = multi_config['inputs']['key']
    local_path = multi_config['inputs']['file_path']
    downloads = local_path + multi_config['inputs']['downloads']
    config = multi_config['config']
    new_file = local_path + multi_config['inputs']['new_files']

    # The file that will contain the list of new files
    output_file = open(new_file, 'w')
    # Erase old lines in the file
    output_file.truncate()

    if not os.path.isdir(downloads):
        print "Missing Downloads Directory. "
        return

    source_files = get_file_list(None, url + '/', key,
        config['dataset_source_id'])

    # Filter out any previously parsed files
    for source_file in source_files:
        file_status = filter_files(url, key, downloads, source_file)
        if file_status is True:
            output_file.write(source_file['filename'] + '\n')

    # Ensure the files are closed properly regardless
    output_file.close()


def filter_files(host, key, downloads, original_file):
    """Only parse the new files"""

    # check to see if this file already exists (i.e.: was previously parsed)
    path_to_file = os.path.join(downloads, original_file['filename'])

    if os.path.isfile(path_to_file):
        print ('%s was already present. Do not parse. Skipping file. '
               % original_file['filename'])
        return False
    else:
        path_to_file = download_file(host, key, original_file['id'],
            os.path.join(downloads, original_file['filename']))
        print("Will get new file %s" % path_to_file)
        return True


def download_file(host, key, file_id, download_file_path):
    """Download the file"""

    if not file_id:
        raise AttributeError('file_id not provided')
    if not download_file_path:
        raise AttributeError('download_file_path not provided')

    url = '%s/api/files/%s?key=%s' % (host, file_id, key)
    r = requests.get(url)

    with open(download_file_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    return download_file_path


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
