# SAS SWAT Developer How-To

Developing SWAT using the REST interface is just like developing any 
other project on GitHub.  You clone the project, do your work, 
and submit a pull request.  However, the binary interface is a bit
different since it requires the bundled SAS TK libraries and Python
C extension modules.

## Developing Against the Binary CAS Interface

In order to run against CAS using the binary interface, you must copy
the C libraries from a platform-specific distribution to your git
clone.  These files are located in the swat/lib/<platform>/ directory.
So to develop on Linux, you would clone SWAT from GitHub, download the
Linux-specific tar.gz file, unzip it, and copy the swat/lib/linux/\*.so
files to your clone directory.  From that point on, you should be able
to connect to both REST and binary CAS ports from your clone.

## Submitting a Pull Request

Submitting a pull request uses the standard process at GitHub.
Note that in the submitted changes, there must always be a unit test
for the code being contributed.  Pull requests that do not have a
unit test will not be accepted.

You also must include the text from the ContributerAgreement.txt file
along with your sign-off verifying that the change originated from you.

## Testing

For the most part, testing the SAS SWAT package is just like testing
any other Python package.  Tests are written using the standard unittest
package.  All test cases are subclasses of TestCase.  Although we do
define our own TestCase class in swat.utils.testing so that we can add
extended functionality.

Since CAS is a network resource and requires authentication, there is
some extra setup involved in getting your tests configured to run 
against your CAS server.  Normally this involves setting the following
environment variables.

* CASHOST - the hostname or IP address of your CAS server (Default: None)
* CASPORT - the port of your CAS server (Default: None)
* CASPROTOCOL - the protocol being using ('cas', 'http', 'https' or 'auto'; Default: 'cas')

* CASUSER - the CAS account username (Default: None)
* CASPASSWORD - the CAS account password (Default: None)

* CASDATALIB    - the CASLib where data sources are found (Default: CASTestTmp)
* CASMPPDATALIB - the CASLib on MPP servers where the data sources are found (Default: HPS)
* CASOUTLIB     - the CASLib to use for output CAS tables (Default: CASUSER)
* CASMPPOUTLIB  - the CASLib to use for output CAS tables on MPP servers (Default: CASUSER)

Some of these can alternatively be specified using configuration files.
The CASHOST, CASPORT, and CASPROTOCOL variables can be specified in a .casrc
in your home directory (or in any directory from the directory you are 
running from all the way up to your home directory).  It is actually written
in Lua, but the most basic form is as follows:

    cashost = 'myhost.com'
    casport = 5570
    casprotocol = 'cas'

The CASUSER and CASPASSWORD variables are usually extracted from your
`~/.authinfo` file automatically.  The only reason you should use environment
variables is if you have a generalized test running account that is
shared across various tools.

Finally, the CAS*DATALIB and CAS*OUTLIB variables configured where your
data sources and output tables reside.  Using the CASDATALIB location 
will make your tests run more efficiently since the test cases can load
the data from a server location.  If you don't specify a CASDATALIB (or
the specified one doesn't exist), the data files will be uploaded to the
server for each test (which will result in hundreds of uploads).  Most
people will likely set them all to CASUSER and create a directory called
`datasources` in their home directory with the contents of the 
`swat/tests/datasources/` directory.

Once you have these setup, you can use tools like nosetest to run the suite:

    nosetests -v swat.tests

You can also run each test individually using profiling as follows:

    python swat/tests/cas/test_basics.py --profile
