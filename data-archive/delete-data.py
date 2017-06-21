import os
import time
import boto3
import subprocess
import pymysql.cursors
from zipfile import ZipFile

# Variables Declaration
user='root'
password='root'
bucket_name = "alaya-files-manan"
file_name = "test-data.txt"
encrypt_key = os.urandom(32)
timestamp=time.strftime('%Y-%m-%d_%H:%M:%S')

file_path ="dumps/"
file_timestamp = "mysql_dump_{}".format(timestamp)
file_name = file_path + file_timestamp
sql_file_name = file_name + ".sql"
zip_file_name = file_name + ".zip"
print(file_name)


# Database Connectivity
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             db='info',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
print("\nConnected to Database!!\n\n")
try:
    with connection.cursor() as cursor:

# Dispaly all the records
        sql = "SELECT id, password FROM tbl_sys_events"
        cursor.execute(sql)
        for row in cursor:
            print("Data that is present inside table:")
            print(row)

# Provides the maximum id from database:
        maxid = "SELECT MAX(id) AS max_id FROM tbl_sys_events"
        cursor.execute(maxid)
        for row in cursor:
            print("\nHighest ID in the table is: ", row["max_id"], "\n\n")
            id_delete = row["max_id"]

            if  id_delete==None:
                print("There are no entries in the table.\n")

# Dump the mysql data
            else:
#                mysqldump = "mysqldump -u {} -p{} --databases info --where 'id<={}' --no-create-info > dumps/mysql_dump_{}".format(user, password, id_delete, timestamp)
                mysqldump = "mysqldump -u {} -p{} --databases info --where 'id<={}' --no-create-info > {}".format(user, password, id_delete, sql_file_name)
                subprocess.run([mysqldump], shell=True)
                with ZipFile(zip_file_name, "w") as myzip:
                    myzip.write("dumps" + "/" + file_timestamp + ".sql", arcname=file_timestamp +".sql")

                print("Data has been backed up until id", id_delete, ", Please proceed to delete the data from server.\n\n")

# Upload Data to AWS S3

                s3 = boto3.client('s3')
                data = open(sql_file_name, "rb")
                print(("Encryption key is"), encrypt_key)
                s3.put_object(Bucket=bucket_name,
                              Key=sql_file_name,
                              Body=data,
                              #ServeyerSideEncryption='AES256',
                              SSECustomerKey=encrypt_key,
                              SSECustomerAlgorithm='AES256')
                print("File uploaded to S3")

# Delete Data from tables
                input_confirm=input("Would you like to proceed in order to delete the data (yes/no)? :")
                if input_confirm.lower() == "yes":
                    delete_data = "DELETE FROM tbl_sys_events WHERE id<={}".format(id_delete)
                    cursor.execute(delete_data)
                    connection.commit()
                    print(("\n\nAll rows are deleted until ID: "),(id_delete))
                else:
                    print("Well, You chose not to delete the data, but it already has been backed up for yor record.")


finally:
    connection.close()
