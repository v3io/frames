# arrow development

## Docker Development

### Build Docker Image

Run this if environment changes
    $ docker build -f Dockerfile.arrow-dev -t frames-arrow-dev .

### Run Docker Image
    $ docker run -v ${PWD}:/frames --rm -it frames-arrow-dev


## Local Environment

**Note: This currently does not work on OSX**

What's you'll need for working with arrow:
- c++ compiler
- arrow libraries

For arrow libraries we're using [miniconda](https://docs.conda.io/en/latest/miniconda.html)

    $ conda create -n frames conda-forge::pyarrow=0.14 conda-forge::compilers pkg-config

Now you need to set environment to pick up arrow libraries (change to where conda enviroments are, see `conda info`)
    $ export CONDA_ENV=${HOME}/.conda/envs/frames
    $ export PKG_CONFIG_PATH=${CONDA_ENV}/lib/pkgconfig
    $ export LD_LIBRARY_PATH=${CONDA_ENV}/lib
    $ export PATH=${CONDA_ENV}/bin:${PATH}


To check run
    $ make frames-arrow
