"""Create Parameters"""
""" python create_parameter_and_mapping.py -c /Users/aarajh/pygeotemporal-parsers/EPA_GLM/pythoplankton.yaml """


import argparse
import yaml
import json
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.client import GeostreamsClient
from requests.exceptions import RequestException
import pandas as pd
import requests
import logging

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1



def main():
    """Main Function"""

    # Required argument for the parser
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', "--config",
                    help="Path to the Multiple Config File",
                    default=r'/multi_config.yml',
                    required=True)
    opts = ap.parse_args()
    if not opts.config:
        ap.print_usage()
        quit()

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    multi_config = yaml.load(open(opts.config, 'r'))
    host = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    parameters_file = multi_config['new_parameters']

    parameters = pd.read_csv(parameters_file)
    
    parameters_client = GeostreamsClient(host=host, username=user, password=password)

    # Creating Parameter
    create_parameters(parameters, parameters_client)


def create_parameters(parameters, parameters_client):
    """Create all sensors and streams"""

    print("Creating parameters...")
    for index, data in parameters.iterrows():
        param_body = {'name': data['name'], 'unit': data['unit'], 'explore_view': True, 'search_view': True, "scale_names": None, "scale_colors": None}
        categories_body = {'name': data['category'], 'detail_type': data['detail_type']}
        post_data = {'parameter':param_body,'categories': categories_body}
        print(post_data)
        try:
            res = json.loads(parameters_client.post("/parameters", post_data).text)
            print(res)
            param_id = res['parameter']['id']
            print ("Parameter %s created [ID: %d]" % (key, param_id))
        except RequestException as e:
            logging.error(f"Error adding parameter {key}: {e}")
            raise e
    return


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("ERROR")
