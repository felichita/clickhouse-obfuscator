#!/usr/bin/python3

import argparse
from clickhouse_driver import Client
from collections import namedtuple
import os
import subprocess

def cmd_args():
    parser = argparse.ArgumentParser(description='Clickhouse obfuscataor')
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost', help='Host of clickhouse-server')
    parser.add_argument('-p', '--port', dest='port', type=int, default=9000, help='Port of clickhouse-server')
    parser.add_argument(
        '-d', '--database', dest='database', type=str, default='', help='Database name of clickhouse-server'
    )
    parser.add_argument(
        '-l', '--limit', dest='limit', type=int,
        default=1000, help='Limit data from specific table of clickhouse-server'
    )
    parser.add_argument(
        '-U', '--user', dest='username', type=str,
        default='default', help='Username for access of clickhouse-server'
    )
    parser.add_argument(
        '-W', '--password', dest='password', default='default',
        type=str, help='Username for access of clickhouse-server'
    )
    return parser.parse_args()

StructTable = namedtuple('StructTable', ['field_name', 'field_type'])

class Clickhouse:

    @staticmethod
    def get_tables_name(args):
        get_tables_command = [
            'clickhouse-client', '--host', f'{args.host}', '--port', f'{args.port}',
            '--database', f'{args.database}', '--user', f'{args.username}', '--password', f'{args.password}',
            '--query', f'"SHOW TABLES;"'
        ]
        pre_result = subprocess.check_output(' '.join(get_tables_command), shell=True)
        return [element.decode('utf-8') for element in pre_result.split() if element.decode('utf-8') != '.']

    @staticmethod
    def get_specific_struct_table(conn, table):
        pre_result = conn.execute(f'DESCRIBE TABLE {table};')
        return [StructTable(field_name=field_name, field_type=field_type)
                for field_name, field_type, _, _, _, _, _ in pre_result]

    @staticmethod
    def get_data_from_columns(args, table_name):
        obfuscate_command = [
            'clickhouse-client', '--host', f'{args.host}', '--port', f'{args.port}',
            '--database', f'{args.database}', '--user', f'{args.username}', '--password', f'{args.password}',
            '--query', f'"SELECT * FROM {table_name} LIMIT {args.limit};"', ">", f'{table_name}.tsv'
        ]
        os.system(' '.join(obfuscate_command))

    @staticmethod
    def cast_struct_table_name_to_str(table):
        return '\t'.join(element.field_name for element in table)

    @staticmethod
    def cast_struct_table_type_to_str(struct_tables):
        arr_table_type = []
        for table in struct_tables:
            arr_table_type.append(f'{table.field_name} {table.field_type}')
        return ', '.join(arr_table_type)

    @staticmethod
    def save_data(table_name, data):
        arr_2d_string = [[str(value) for value in struct] for struct in data]
        arr_string = []
        for element in arr_2d_string:
            arr_string.append('\t'.join(element))
        with open(f'{table_name}.tsv', 'w+') as f:
            for line in arr_string:
                f.write(line)
                f.write(os.linesep)

    @staticmethod
    def truncate_tables(conn, table):
        conn.execute(f'TRUNCATE TABLE {table};')

    @staticmethod
    def insert_tables(args, table_name):
        insert_command = [
            'clickhouse-client', '--host', f'{args.host}', '--port', f'{args.port}',
            '--database', f'{args.database}', '--user', f'{args.username}', '--password', f'{args.password}',
            '--query', f'"INSERT INTO {table_name} FORMAT TSV"', "<", f'{table_name}_new.tsv'
        ]
        os.system(' '.join(insert_command))

    @staticmethod
    def obfuscate_data(table_name, field_types):
        obfuscate_command = [
            'clickhouse-obfuscator', '--seed', '"$(head -c16 /dev/urandom | base64)"',
            '--input-format', 'TSV', '--output-format', 'TSV', '--structure',
            f"'{field_types}'", "<", f"{table_name}.tsv", ">", f"{table_name}_new.tsv"
        ]
        os.system(' '.join(obfuscate_command))

def main():
    args = cmd_args()
    clickhouse = Client(
        host=args.host,
        database=args.database,
        user=args.username,
        port=args.port,
        password=args.password,
        verify=False
    )

    tables = Clickhouse.get_tables_name(args)
    for table in tables:
        struct_columns = Clickhouse.get_specific_struct_table(clickhouse, table)
        Clickhouse.get_data_from_columns(args, table)
        result_type = Clickhouse.cast_struct_table_type_to_str(struct_columns)
        Clickhouse.obfuscate_data(table, result_type)
        Clickhouse.truncate_tables(clickhouse, table)
        Clickhouse.insert_tables(args, table)

if __name__ == '__main__':
    main()
