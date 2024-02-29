#  Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT-0

# Work with Python 3 in Glue 2.0 and Glue 3.0
from __future__ import print_function

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

from hive_metastore_migration import *


def transform_df_to_catalog_import_schema(sql_context, glue_context, df_databases, df_tables, df_partitions,
                                          db_prefix, table_prefix, filter):
    
    # Add filter feature here
    df_databases_filter = df_databases.limit(0)
    df_tables_filter = df_tables.limit(0)
    df_partitions_filter = df_partitions.limit(0)
    if filter:
        filter_list = filter.split(',')
        for filter_item in filter_list:
            filter_db = filter_item.split('.')[0]
            filter_table = filter_item.split('.')[1]

            print('filter: ' + filter_db + '.' + filter_table)
            if db_prefix:
                filter_db = db_prefix + filter_db
            if table_prefix:
                filter_table = table_prefix + filter_table
            print('filter with prefix: ' + filter_db + '.' + filter_table)
            #filter db
            df_databases_filter = df_databases_filter.union(df_databases.filter(col("item.name").like(filter_db)))
            #filter table
            df_tables_filter = df_tables_filter.union(df_tables.filter(
                col("database").like(filter_db) & col("item.name").like(filter_table)))
            #filter partition
            df_partitions_filter = df_partitions_filter.union(df_partitions.filter(
                col("database").like(filter_db) & col("table").like(filter_table)))
    else:
        df_databases_filter = df_databases
        df_tables_filter = df_tables
        df_partitions_filter = df_partitions


    df_databases_filter = df_databases_filter.withColumn("temp", 
                                                         concat(col("item.name"))
                                                         ).dropDuplicates(subset=["temp"]).drop("temp")

    df_databases_array = df_databases_filter.select(df_databases['type'], array(df_databases['item']).alias('items'))
    
    #df_databases_array.printSchema()
    #df_databases_array.show(truncate=False)
    
    df_tables_filter = df_tables_filter.withColumn("temp", 
                                                   concat(col("database"),col("item.name"))
                                                   ).dropDuplicates(subset=["temp"]).drop("temp")


    df_tables_array = df_tables_filter.select(df_tables['type'], df_tables['database'],
                                       array(df_tables['item']).alias('items'))
    #df_tables_array.printSchema()
    #df_tables_array.show(truncate=False)                                 
    #df_partitions_filter.printSchema()
    #df_partitions_filter.show(truncate=False)

    df_partitions_filter = df_partitions_filter.withColumn("temp", 
        concat(array(col("database"),col("table")),col("item.values"))).dropDuplicates(subset=["temp"]).drop("temp")

    df_partitions_array_batched = batch_metastore_partitions(sql_context=sql_context, df_parts=df_partitions_filter)
    
    #df_partitions_array_batched.printSchema()
    #df_partitions_array_batched.show(truncate=False)

    dyf_databases = DynamicFrame.fromDF(
        dataframe=df_databases_array, glue_ctx=glue_context, name='dyf_databases')
    dyf_tables = DynamicFrame.fromDF(
        dataframe=df_tables_array, glue_ctx=glue_context, name='dyf_tables')
    dyf_partitions = DynamicFrame.fromDF(
        dataframe=df_partitions_array_batched, glue_ctx=glue_context, name='dyf_partitions')
    return dyf_databases, dyf_tables, dyf_partitions


def import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region,
                       db_prefix, table_prefix, filter):

    (dyf_databases, dyf_tables, dyf_partitions) = transform_df_to_catalog_import_schema(
        sql_context, glue_context, databases, tables, partitions,db_prefix, table_prefix, filter)

    # load
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_databases, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_tables, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})
    glue_context.write_dynamic_frame.from_options(
        frame=dyf_partitions, connection_type='catalog',
        connection_options={'catalog.name': datacatalog_name, 'catalog.region': region})


def metastore_full_migration(sc, sql_context, glue_context, connection, datacatalog_name, db_prefix, table_prefix
                             , region, filter):
    # extract
    hive_metastore = HiveMetastore(connection, sql_context)
    hive_metastore.extract_metastore()

    # transform
    (databases, tables, partitions) = HiveMetastoreTransformer(
        sc, sql_context, db_prefix, table_prefix).transform(hive_metastore)

    #load
    import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region, 
                       db_prefix, table_prefix, filter)


def metastore_import_from_s3(sql_context, glue_context,db_input_dir, tbl_input_dir, parts_input_dir,
                             datacatalog_name, region):

    # extract
    databases = sql_context.read.json(path=db_input_dir, schema=METASTORE_DATABASE_SCHEMA)
    tables = sql_context.read.json(path=tbl_input_dir, schema=METASTORE_TABLE_SCHEMA)
    partitions = sql_context.read.json(path=parts_input_dir, schema=METASTORE_PARTITION_SCHEMA)

    # load
    import_datacatalog(sql_context, glue_context, datacatalog_name, databases, tables, partitions, region)


def main():
    # arguments
    from_s3 = 'from-s3'
    from_jdbc = 'from-jdbc'
    parser = argparse.ArgumentParser(prog=sys.argv[0])
    parser.add_argument('-m', '--mode', required=True, choices=[from_s3, from_jdbc], help='Choose to migrate metastore either from JDBC or from S3')
    parser.add_argument('-c', '--connection-name', required=False, help='Glue Connection name for Hive metastore JDBC connection')
    parser.add_argument('-R', '--region', required=False, help='AWS region of target Glue DataCatalog, default to "us-east-1"')
    parser.add_argument('-d', '--database-prefix', required=False, help='Optional prefix for database names in Glue DataCatalog')
    parser.add_argument('-t', '--table-prefix', required=False, help='Optional prefix for table name in Glue DataCatalog')
    parser.add_argument('-D', '--database-input-path', required=False, help='An S3 path containing json files of metastore database entities')
    parser.add_argument('-T', '--table-input-path', required=False, help='An S3 path containing json files of metastore table entities')
    parser.add_argument('-P', '--partition-input-path', required=False, help='An S3 path containing json files of metastore partition entities')
    parser.add_argument('-F', '--filter', 
            required=False, 
            help='Filter for database and table, use db.table to filter, support like wildcard: db%.table%, use comma for multiple filters, e.g. db1.table1,db2.table2,db3%.table3%') 

    options = get_options(parser, sys.argv)
    if options['mode'] == from_s3:
        validate_options_in_mode(
            options=options, mode=from_s3,
            required_options=['database_input_path', 'table_input_path', 'partition_input_path'],
            not_allowed_options=['database_prefix', 'table_prefix']
        )
    elif options['mode'] == from_jdbc:
        validate_options_in_mode(
            options=options, mode=from_jdbc,
            required_options=['connection_name'],
            not_allowed_options=['database_input_path', 'table_input_path', 'partition_input_path']
        )
    else:
        raise AssertionError('unknown mode ' + options['mode'])

    validate_aws_regions(options['region'])

    # spark env
    (conf, sc, sql_context) = get_spark_env()
    glue_context = GlueContext(sc)

    # launch job
    if options['mode'] == from_s3:
        metastore_import_from_s3(
            sql_context=sql_context,
            glue_context=glue_context,
            db_input_dir=options['database_input_path'],
            tbl_input_dir=options['table_input_path'],
            parts_input_dir=options['partition_input_path'],
            datacatalog_name='datacatalog',
            region=options.get('region') or 'us-east-1'
        )
    elif options['mode'] == from_jdbc:
        glue_context.extract_jdbc_conf(options['connection_name'])
        print('db_prefix_1:'  + (options.get('database_prefix') or ''))
        metastore_full_migration(
            sc=sc,
            sql_context=sql_context,
            glue_context=glue_context,
            connection=glue_context.extract_jdbc_conf(options['connection_name']),
            db_prefix=options.get('database_prefix') or '',
            table_prefix=options.get('table_prefix') or '',
            datacatalog_name='datacatalog',
            region=options.get('region') or 'us-east-1',
            filter=options.get('filter') or ''
        )

if __name__ == '__main__':
    main()