"""Create Parameters"""
""" python create_parameter_and_mapping.py -c /Users/aarajh/pygeotemporal-parsers/EPA_GLM/pythoplankton.yaml """


import argparse
import yaml
import json
from pygeotemporal.sensors import SensorsApi
from pygeotemporal.streams import StreamsApi
from pygeotemporal.client import GeostreamsClient
from requests.exceptions import RequestException

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

    multi_config = yaml.load(open(opts.config, 'r'))
    host = multi_config['inputs']['location']
    user = multi_config['inputs']['user']
    password = multi_config['inputs']['password']
    parameters = multi_config['new_parameters']
    
    parameters_client = GeostreamsClient(host=host, username=user, password=password)

    # Creating Parameter
    create_parameters(parameters, parameters_client)


def create_parameters(parameters, parameters_client):
    """Create all sensors and streams"""

    print("Creating parameters...")
    for key, value in parameters.items():
        param_body = {'name': key}
        categories_body = value.pop('categories')
        # Set explore_view to false if not provided in yaml
        if not hasattr(value, 'explore_view'):
            value['explore_view'] = False
        # Set search_view to false if not provided in yaml
        if not hasattr(value, 'search_view'):
            value['search_view']= False
        param_body.update(value)

        post_data = {'parameter':param_body,'categories': categories_body}

        try:
            param_id = json.loads(parameters_client.post("/parameters", post_data).text)['parameter']['id']
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
