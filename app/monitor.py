#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Dump all replication events from a remote mysql server
#

import datetime
import pika
import json
import os.path
import os

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)


class DateEncoder(json.JSONEncoder):
    """
    自定义类，解决报错：
    TypeError: Object of type 'datetime' is not JSON serializable
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        else:
            return json.JSONEncoder.default(self, obj)


# 数据库连接配置
MYSQL_SETTINGS = {
    "host": os.getenv('ENV_BINLOG_HOST'),
    "port": int(os.getenv('ENV_BINLOG_PORT')),
    "user": os.getenv('ENV_BINLOG_USER'),
    "passwd": os.getenv('ENV_BINLOG_PASSWD')
}


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv('ENV_MQ_HOST'),
            port=int(os.getenv('ENV_MQ_PORT')),
            credentials=pika.PlainCredentials(
                os.getenv('ENV_MQ_USER'),
                os.getenv('ENV_MQ_PASSWD')
            ),
            virtual_host='/'
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue='default_queue', durable=True)

    file_name = "file_pos.log"
    log_file = ''
    log_pos = 0
    if os.path.isfile(file_name):
        fo = open(file_name, "r")
        file_pos = fo.read()
        fo.close()
        if file_pos != '':
            fp_list = file_pos.split('|')
            log_file = fp_list[0]
            log_pos = int(fp_list[1])

    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                blocking=True,
                                resume_stream=True,
                                log_file=log_file,
                                log_pos=log_pos,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                only_tables=['system_message'])

    for binlogevent in stream:
        # binlogevent.dump()  # 打印所有信息

        for row in binlogevent.rows:
            # 打印 库名 和 表名
            event = {"schema": binlogevent.schema,
                     "table": binlogevent.table,
                     # "log_pos": stream.log_pos,
                     # "log_file": stream.log_file
                     }

            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event["data"] = row["values"]

            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event["data"] = row["after_values"]  # 注意这里不是values

            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event["data"] = row["values"]

            print(json.dumps(event, cls=DateEncoder))

            message = {
                'class': '\\MZ\\Models\\user\\UserModel',
                'method': 'getUserById',
                'data': event
            }

            body = json.dumps(message, cls=DateEncoder)

            channel.basic_publish(exchange='default_ex', routing_key='default_route', body=body)

            fo = open(file_name, "w")
            fo.write(stream.log_file + '|' + str(stream.log_pos))
            fo.close()

            # sys.stdout.flush()

    # stream.close()  # 如果使用阻塞模式，这行多余了


if __name__ == "__main__":
    main()
