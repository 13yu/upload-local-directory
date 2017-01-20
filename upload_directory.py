#!/usr/bin/env python2
# coding: utf-8
import datetime
import logging
import os
import time

import boto3
import s3transfer
from botocore.client import Config

access_key = 'ziw5dp1alvty9n47qksu'
secret_key = 'V+ZTZ5u5wNvXb+KP5g0dMNzhMeWe372/yRKx4hZV'
bucket_name = 'renzhi-test-bucket'
file_acl = 'public-read'


report_interval = 30


mega = 1024.0 * 1024.0


schedule = {
    'start': '22:25',
    'stop': '3:30',
}

stat = {
    'bytes_uploaded': 0,
    'start_time': time.time(),
    'bandwidth': 10,  # 10M
    'report_time': time.time(),
}


config = Config(signature_version='s3v4')
s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=config,
    region_name='us-east-1',
    endpoint_url='http://127.0.0.1',
)
s3_transfer = s3transfer.S3Transfer(s3_client)


def filter_dir(dir_name):
    if dir_name.startswith('.'):
        return False

    return True


def filter_file(file_name):
    if file_name.startswith('.'):
        return False

    return True


def get_iso_now():
    datetime_now = datetime.datetime.utcnow()
    return datetime_now.strftime('%Y%m%dT%H%M%SZ')


def dir_iter(dir_name):
    q = []
    base_dir = dir_name.split('/')
    q.append(base_dir)

    while True:
        if len(q) < 1:
            break
        dir_parts = q.pop(0)

        files = os.listdir('/'.join(dir_parts))
        for f in files:
            _dir_parts = dir_parts[:]
            _dir_parts.append(f)

            if filter_dir(f) == False:
                continue

            if os.path.isdir('/'.join(_dir_parts)):
                q.append(_dir_parts)

        yield dir_parts


def get_files_to_upload(dir_name, progress_file):

    files = os.listdir(dir_name)
    files_to_upload = {}
    for f in files:
        if filter_file(f) == False:
            continue
        file_name = os.path.join(dir_name, f)
        files_to_upload[file_name] = True

    fd = open(progress_file, 'a')
    fd.close()

    fd = open(progress_file)
    while True:
        line = fd.readline()
        if line == '':
            break
        file_name = line.split()[0]
        if file_name in files_to_upload:
            files_to_upload.pop(file_name)

    fd.close()

    return files_to_upload


def upload_one_file(file_name, base_len, key_prefix):
    file_parts = file_name.split('/')
    key = os.path.join(key_prefix, '/'.join(file_parts[base_len:]))

    info = {}

    if os.path.isdir(file_name):
        info['local_size'] = 0

        key = key + '/'
        resp = s3_client.put_object(
            ACL=file_acl,
            Bucket=bucket_name,
            Key=key,
            Body=''
        )

        status = resp['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            logger.error('failed to put object: %s %d' % (key, status))
            return

        resp = s3_client.head_object(
            Bucket=bucket_name,
            Key=key
        )
        status = resp['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            logger.error('failed to put object: %s %d' % (key, status))
            return

        info['file_key'] = key
        info['etag'] = resp['ETag']
        info['resp_size'] = resp['ContentLength']

    else:
        info['local_size'] = os.stat(file_name).st_size

        s3_transfer.upload_file(file_name, bucket_name,
                                key, extra_args={'ACL': file_acl})

        resp = s3_client.head_object(
            Bucket=bucket_name,
            Key=key
        )
        status = resp['ResponseMetadata']['HTTPStatusCode']
        if status != 200:
            logger.error('failed to put object: %s %d' % (key, status))
            return

        info['file_key'] = key
        info['etag'] = resp['ETag']
        info['resp_size'] = resp['ContentLength']

    info['upload_time'] = get_iso_now()

    return info


def upload_one_directory(dir_parts, base_len, key_prefix):
    dir_name = '/'.join(dir_parts)
    progress_file = os.path.join(dir_name, '.upload_progress_')

    files_to_upload = get_files_to_upload(dir_name, progress_file)

    fd = open(progress_file, 'a')
    for file_name in files_to_upload.keys():
        logger.warn('start to upload file: %s' % file_name)

        info = upload_one_file(file_name, base_len, key_prefix)
        if info == None:
            continue

        if info['local_size'] != info['resp_size']:
            logger.error('file size not equal, local_size: %d, response size: %d'
                         % (info['local_size'], info['resp_size']))

        upload_time = get_iso_now()
        line = '%s %s %s %d %d %s\n' % (file_name, info['file_key'], info['etag'],
                                        info['local_size'], info['resp_size'], upload_time)
        fd.write(line)

        stat['bytes_uploaded'] = stat['bytes_uploaded'] + info['local_size']
        time_need = stat['bytes_uploaded'] / (mega * stat['bandwidth'])
        ts_now = time.time()
        time_to_sleep = stat['start_time'] + time_need - ts_now

        if ts_now - stat['report_time'] > report_interval:
            stat['report_time'] = ts_now
            time_used = ts_now - stat['start_time']
            report_str = ('upload stat, bytes uploaded: %dM, time used: %fs, bandwidth: %f Mbytes/s'
                          % (stat['bytes_uploaded'] / mega, time_used,
                              stat['bytes_uploaded'] / time_used / mega))
            logger.warn(report_str)
            print report_str

        if time_to_sleep > 0:
            logger.warn('about to sleep %f seconds to slow down' %
                        time_to_sleep)
            time.sleep(time_to_sleep)

        check_schedule()

    fd.close()


def run(dir_name, key_prefix):
    if dir_name.endswith('/'):
        print 'do not add / to the directory name: ' + dir_name
        return

    if not dir_name.startswith('/'):
        print 'the directory name is not absolute path: ' + dir_name
        return

    if not os.path.exists(dir_name) or not os.path.isdir(dir_name):
        print dir_name + ' is not exists or is not a directory'
        return

    base_len = len(dir_name.split('/')) - 1

    print 'start to upload ' + dir_name + ' to ' + key_prefix
    for dir_parts in dir_iter(dir_name):
        upload_one_directory(dir_parts, base_len, key_prefix)


def check_schedule():
    start_h = int(schedule['start'].split(':')[0])
    start_m = int(schedule['start'].split(':')[1])
    stop_h = int(schedule['stop'].split(':')[0])
    stop_m = int(schedule['stop'].split(':')[1])

    start_m = start_m + start_h * 60
    stop_m = stop_m + stop_h * 60

    while True:
        now = datetime.datetime.now()
        now_h = now.hour
        now_m = now.minute

        now_m = now_m + now_h * 60

        if start_m < stop_m:
            if now_m >= start_m and now_m <= stop_m:
                return
            else:
                wait_m = (start_m - now_m) % (60 * 24)
                line = 'the schedule is from %s to %s, need to wait %d hours and %d minutes' % (
                    schedule['start'], schedule['stop'], wait_m / 60, wait_m % 60)

                print line
                logger.warn(line)
                time.sleep(60)

        else:
            if now_m > stop_m and now_m < start_m:
                wait_m = (start_m - now_m) % (60 * 24)
                line = 'the schedule is from %s to %s, need to wait %d hours and %d minutes' % (
                    schedule['start'], schedule['stop'], wait_m / 60, wait_m % 60)

                print line
                logger.warn(line)
                time.sleep(60)
            else:
                return


if __name__ == "__main__":

    import sys

    dir_name = sys.argv[1]
    key_prefix = sys.argv[2]
    log_dir = sys.argv[3]
    bandwidth = sys.argv[4]

    stat['bandwidth'] = float(bandwidth)

    if not os.path.exists(log_dir) or not os.path.isdir(log_dir):
        print log_dir + ' is not exists or is not a directory'
        exit()

    log_file = os.path.join(log_dir, 'upload-log-for-' +
                            dir_name.replace('/', '_') + '.log')

    logger = logging.getLogger()
    logger.setLevel(logging.WARN)
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    run(dir_name, key_prefix)
