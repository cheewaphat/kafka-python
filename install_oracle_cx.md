Linux

Step 1:
sudo apt-get install build-essential unzip python-dev libaio-dev

Step 2. Click here to download the appropriate zip files required for this. You'll need:

instantclient-basic-linux
instantclient-sdk-linux

Get the appropriate version for your system.. x86 vs 64 etc. Make sure you don't get version 12, since it's not supported by the cx_Oracle moduel yet.

Unzip the content in the same location, so you'll end up with a folder named: instantclient_11_2 (in my case) which will contain a bunch of .so and jar files.

just for ease of use I'll use $ORACLE_HOME which will basically point to the location where you unzipped your installclient folders.

export ORACLE_HOME=$(pwd)/instantclient_11_2
Step 3. create a symlink to your SO file.

cd $ORACLE_HOME
ln -s libclntsh.so.11.1   libclntsh.so  #the version number on your .so file might be different
Step 4. Update your /etc/profile or your ~/.bashrc

export ORACLE_HOME=/location/of/your/files/instantclient_11_2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME
Step 5: Edit /etc/ld.so.conf.d/oracle.conf

This is a new file, simple add the location of your .so files here, then update the ldpath using

sudo ldconfig
Step 6. Finaly just install cx_oracle module:

pip install cx_oracle