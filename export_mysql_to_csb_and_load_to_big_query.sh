#!/usr/bin/env bash

#title           :export_db_and_load_to_bq.sh
#description     :Run this script to export database to csv tables, then copy
# tables to cloud storage bucket and finally load the tables to bigquery database.
#author		 :  Ari Lopez
#date            :11-01-2019
#version         :1.0
#usage		 :bash export_db_and_load_to_bq.sh -run
#Requirements:
#Bash and mysql installed. SSL database certificates for encrypted connection
# gcloud installed logged in and set with the appropriate gcp account
# Python to run the script that will filter and export the data
# Set the following parameters:
host="<HOST-IPADDRESS>"
user="<DATABASE-USER>"
pass="<DATABASE-PASSWORD>"
database="<DATABASE>"
ca="<CA.PEM>"
cert="<CERT.PEM>"
key="<KEY.PEM>"
path_to_export_tables="<CSV-TABLES-LOCATION>"
bucket="<BUCKET-TO-STORE-CSV-TABLES>"
dataset="<BQUERY-SCHEMA>"


usage() {
echo "Open the script and set the following parameters:"
echo '
host="<HOST-IPADDRESS>"
user="<DATABASE-USER>"
pass="<DATABASE-PASSWORD>"
database="<DATABASE>"
ca="<CA.PEM>"
cert="<CERT.PEM>"
key="<KEY.PEM>"
path_to_export_tables="<CSV-TABLES-LOCATION>"
bucket="<BUCKET-TO-STORE-CSV-TABLES>"
dataset="<BQUERY-SCHEMA>"'
echo ""
echo "Usage: $0 [-run] " 1>&2; exit 1;
}

if [ "$#" -lt 1 ]; then
    usage
fi
if [ $1 != "-run" ]; then
  usage
  exit
fi

mkdir -p $path_to_export_tables

rm -rf $path_to_export_tables/*

python python/export_and_filter_data.py $host $user $pass $database $ca $cert \
                             $key $path_to_export_tables

tables=($(
mysql \
--ssl-ca=$ca \
--ssl-cert=$cert \
--ssl-key=$key \
-h $host -u $user -p$pass $database -sN -e 'SHOW TABLES;'
))

# declare -a tables=("HsAddress")
bq rm -r -f $dataset
bq mk --data_location=EU $dataset

for table in "${tables[@]}"
do
   cp_storage="gsutil cp $path_to_export_tables/$table.csv $bucket"
   load_bigquery="bq load --autodetect --source_format=CSV $dataset.$table $bucket/$table.csv"
   echo "$(date) : Copying : $table in cloud storage"
   $cp_storage
   echo "$(date) : Loading : $table in bigquery"
   $load_bigquery
done

rm -rf $path_to_export_tables/*.csv
