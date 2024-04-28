Learn nfsdb
===========

The project is for learning nfsdb.

What is nfsdb?
==============

Need For Speed Database. Super-fast, transactional database.

Install Thrift
==============

1.Install Boost
=============

download from `https://www.boost.org/`

```bash
./bootstrap.sh
sudo ./b2 threading=multi address-model=64 variant=release stage install
```

2.Install libevent
====================

download from `https://libevent.org/`

```bash
./configure --prefix=/usr/local 
make
sudo make install
```

3.Build Apache Thrift
===================

download from `https://thrift.apache.org/download`

```bash
./configure --prefix=/usr/local/ --with-boost=/usr/local --with-libevent=/usr/local
make install
```

Link
====

- https://thrift.apache.org/docs/install/os_x.html
- https://stackoverflow.com/questions/10778905/why-does-my-mac-os-x-10-7-3-have-an-old-version-2-3-of-gnu-bison