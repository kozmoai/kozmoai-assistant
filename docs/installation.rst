.. highlight:: shell

============
Installation
============


Stable release
--------------

To install ``kozmoai-assistant``, run this command in your terminal:

.. code-block:: console

    $ python -m pip install kozmoai-assistant

This is the preferred method to install ``kozmoai-assistant``, as it will always install the most recent stable release.

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for ``kozmoai-assistant`` can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone https://github.com/kozmoai/kozmoai-assistant.git

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ cd kozmoai-assistant
    $ poetry install
    $ poetry shell


.. _Github repo: https://github.com/kozmoai/kozmoai-assistant.git

Troubleshooting
---------------

For mac and linux system, there are sometimes slim python installations that do not include the ``kozmoai-assistant`` requirement tkinter, which is a standard library and thus not pip installable.

To install tkinter on mac, you can for example use brew:

.. code-block:: console

    $ brew install python-tk

On debian-based linux systems you can use:

.. code-block:: console

    $ sudo apt-get install python3-tk
