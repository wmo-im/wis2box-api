# wis2box-api

WIS 2.0 in a box OGC API

## Installation

wis2box-api is part of the [wis2box](https://docs.wis2box.wis.wmo.int) project.

### Requirements

* Python 3 and above
* Python [virtualenv](https://virtualenv.pypa.io/) package

### Dependencies

Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during wis2box-api's installation.

### Installing the Package

```bash
python3 -m venv my-env
cd my-env
. bin/activate
git clone https://github.com/wmo-im/wis2box-api.git
cd wis2box-api
python setup.py build
python setup.py install
```

## Running

```bash
export FLASK_APP=wis2box_api.app
flask run
```

## Development

### Setting up a Development Environment

Same as installing a package.  Use a virtualenv.  Also install developer
requirements:

```bash
pip install -r requirements-dev.txt
```

## Releasing

```bash
# create release (x.y.z is the release version)
vi wis2box_api/__init__.py  # update __version__
git commit -am 'update release version x.y.z'
git push origin main
git tag -a x.y.z -m 'tagging release version x.y.z'
git push --tags

# publish release on GitHub (https://github.com/wmo-im/wis2box-api/releases/new)

# bump version back to dev
vi wis2box_api/__init__.py  # update __version__
git commit -am 'back to dev'
git push origin main
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

## Issues

Please direct all issues to the [main wis2box issue tracker](https://github.com/wmo-im/wis2box/issues)

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
* [Maaike Limper](https://github.com/maaikelimper)
