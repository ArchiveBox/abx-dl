__package__ = 'abx_dl'

import sys

# from click import click

def main(*args, **kwargs):
    # get list of all available extractors
    # filter list to selected/viable extractors based on CLI args and env/config vars
    # run each extractor in order on the URL:
    #   - install extractor binary/dependencies lazily if needed
    #   - check if extractor should run on url
    #   - write extractor output to current working directory
    #   - update index.json in current working directory
    print('⬇️ Downloading with abx-dl:', *args)

if __name__ == '__main__':
    # click entrypoint https://click.palletsprojects.com/
    main(*sys.argv)
