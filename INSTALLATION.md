# Installation #

## OSX ##

## Linux ##
(These instructions are only for Ubuntu for the moment)

### Prerequisites ###
Before building the modules in this repository, you will need to make sure that you have the following:
* Python 3.6 with header files (`python3.6-dev python3.6-dbg`)
  Note that for development you will also install the debug interpreter.
  On Ubuntu this is not so straight forward. You will first need to add the following PPA:
  ```
  sudo add-apt-repository ppa:jonathonf/python-3.6
  ```
  Then run as normal:
  ```
  sudo apt install python3.6-dev python3.6-dbg
  ```
* Pipenv ([see this link](https://pipenv.readthedocs.io/en/latest/install/#installing-pipenv))
* Redis Server (`redis-server`)
