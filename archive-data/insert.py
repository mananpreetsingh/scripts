
import pymysql.cursors



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

# Insert Data
        try:
            i=0
            input_number = int(input("How many Rows would you like to generate ?"))
            while i<input_number:
                sql_insert = "INSERT INTO tbl_sys_events (email, password) VALUES (%s, %s)"
                cursor.execute(sql_insert, ("alayacare@python.org", "secret"))
                connection.commit()
                i=i+1
        except:
            print("\n\n Input value is wrong, Please enter an integer.\n\n")

# Display all the records
        print("Following are the current entries in the table :\n\n")
        sql = "SELECT id, password FROM tbl_sys_events"
        cursor.execute(sql)
        for row in cursor:
            print(row)

finally:
    connection.close()
