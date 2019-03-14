# Installation #

## OSX ##

#### Prerequisites ####
* Python 3.6 (recommended installed with homebrew)
  * Currently build is tested against `clang`, not `gcc`. For more information about installing `clang` and configuring your environment see [here](https://embeddedartistry.com/blog/2017/2/20/installing-clangllvm-on-osx)
* It is recommended you use Pipenv ([see this link](https://pipenv.readthedocs.io/en/latest/install/#installing-pipenv)) to manage the application.
  * You can also use virtualenv.
* install Redis (`brew install redis`)




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
