<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#   Table of Content

- [Name](#name)
- [Status](#status)
- [Description](#description)
- [Install](#install)
- [Usage](#usage)
- [Author](#author)
- [Copyright and License](#copyright-and-license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#   Name

upload-local-directory:
A python script used for uploading an entire local directory to s3

#   Status

This library is in beta phase.

#   Description

This script will upload all files in a directory recusively, it will use
a prefix you specified concate with the relative path of the file as the
key of the uploaded file. To sepcify the access key and secret key or the
bucket name, you need to modify the source code.
This script will create a file named `.upload_progress_` in very directory,
it is used to record which file has been uploded, so the script will not
upload those fils again when you run this script more than once.

#   Install

This package does not support installation.

Just clone it, and run it with arguments.

But you need to install boto3 first using the following command

```
    pip install boto3
```

#   Usage

- change the following line in the beginning of the file
`upload_directory.py` to use you own access key and secret key
and you own bucket, you can also change the report interval

```python
access_key = 'ziw5dp1alvty9n47qksu'
secret_key = 'V+ZTZ5u5wNvXb+KP5g0dMNzhMeWe372/yRKx4hZV'
bucket_name = 'renzhi-test-bucket'
report_interval = 30
```

- run the script whit arguments

```
python upload_directory.py '/root/mydir' 'key-prefix' '/tmp' 50
```

arguments:

- `/root/mydir` is the directory you want to upload.

- `key-prefix` is the prefix you want to put before the relative path of
     the file. See the following example.

- `/tmp` is the log dir, this directory must exists.

- `50` is the bandwidth, the unit is megabytes.

example:

if you have the following directory:

```
/root/mydir
     aaa.png
     bbb.png
     ccc.png
     subdir
         aaa.png
         bbb.png
```

and if you use the following arguments


```
python upload_directory.py '/root/mydir' 'key-prefix' '/tmp' 50
```

then the keys in the bucket will be the following:

```
key-prefix/mydir/
key-prefix/mydir/aaa.png
key-prefix/mydir/bbb.png
key-prefix/mydir/ccc.png
key-prefix/mydir/subdir/
key-prefix/mydir/subdir/aaa.png
key-prefix/mydir/subdir/bbb.png
```

Renzhi (任稚) <zhi.ren@baishancloud.com>

#   Copyright and License

The MIT License (MIT)

Copyright (c) 2016 Renzhi (任稚) <zhi.ren@baishancloud.com>
