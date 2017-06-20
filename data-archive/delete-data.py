import subprocess
import time
import pymysql.cursors

user='root'
password='root'
timestamp=time.strftime('%Y-%m-%d_%H:%M:%S')
# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             db='info',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

print("\nDatabase Connection is Successfull!!\n\n")

try:
    with connection.cursor() as cursor:

# Read all the records
        sql = "SELECT id, password FROM tbl_sys_events"
        cursor.execute(sql)
        for row in cursor:
            print(row)

# Gives you the maximum id number in database:

        maxid = "SELECT MAX(id) AS max_id FROM tbl_sys_events"
        cursor.execute(maxid)
        for row in cursor:
            print("\nHighest ID in the table is: ", row["max_id"], "\n\n")
            id_delete = row["max_id"]

            if  id_delete==None:
                print("There are no entries in the table.\n")

#Create the mysql dump
            else:
                mysqldump = "mysqldump -u {} -p{} --databases info --where 'id<={}' --no-create-info > dumps/mysql_dump_{}".format(user, password, id_delete, timestamp)
                subprocess.run([mysqldump], shell=True)
                print("Data has been backed up, Please proceed to delete the data from server.\n\n")


#Delete Data
                input_confirm=input("Would you like to proceed in order to delete the data (yes/no)?")
                if input_confirm.lower() == "yes":
                    delete_data = "DELETE FROM tbl_sys_events WHERE id<={}".format(id_delete)
                    cursor.execute(delete_data)
                    connection.commit()
                    print(("Deleted all rows until ID: "),(id_delete))
                else:
                    print("Well, You chose not to delete the data, but it already has been backed up for yor record.")


finally:
    connection.close()
