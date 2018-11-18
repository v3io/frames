.. v3io_frames documentation master file, created by
   sphinx-quickstart on Sun Nov 18 14:58:04 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to v3io_frames's documentation!
=======================================

``v3io_frames`` is a fast streaming of ``pandas.DataFrame`` to and from various
nuclio_ databases.


.. _nuclio: https://nuclio.io/


.. code-block:: python

   import v3io_frames as v3f

   client = v3f.Client(address='localhost:8080')
   num_dfs = num_rows = 0
   size = 1000
   for df in client.read(backend='weather', table='table', max_in_message=size):
       print(df)
       num_dfs += 1
       num_rows += len(df)

   print('\nnum_dfs = {}, num_rows = {}'.format(num_dfs, num_rows))



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
