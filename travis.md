# Travis Integration

We run integration tests on travis. See `.travis.yml` for details.

In [travis settings](https://travis-ci.org/v3io/frames/settings) we have the
following environment variables defined:

### Docker
- `DOCKER_PASSWORD` Password to push images
- `DOCKER_USERNAME` Username to push images

### PyPI

- `V3IO_PYPI_PASSWORD` Password to push new release to pypi
- `V3IO_PYPI_USER` User to push

### Iguazio

`V3IO_SESSION` is a JSON encoded map with session information to run tests.
Make sure to quote the JSON object with `'`. Here's an example value:

`'{"url":"45.39.128.5:8081","container":"mitzi","user":"daffy","password":"rabbit season"}'`


