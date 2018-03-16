import pymysql.cursors
import time
import datetime

# Connect to the database
connection = pymysql.connect(host='de-application.cef17qxjlavg.us-west-2.rds.amazonaws.com',
                             user='bhandari',
                             password='12345',
                             port=3306,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:

        # Creating table if not exist
        cursor.execute("CREATE TABLE IF NOT EXISTS Bhandari.output_data(Serial_No INT(10) AUTO_INCREMENT, timestamp TIMESTAMP, Column_Name varchar(50), Average float(24,3), Standard_Deviation float(24,3), Median float(24,3), Count int(10), Primary Key (Serial_No));")

        # Checking last row count according to timestamp
        rows_count = cursor.execute("SELECT Count from Bhandari.output_data order by timestamp desc limit 1;")
        if rows_count>0:
            result2 = cursor.fetchone()
            last_count=result2['Count']
        else:
            last_count=0

        # Default Column
        col_name = ["lot_size_sqft", "total_building_sqft", "yr_built", "bedrooms", "total_rooms", "bath_total", "final_value"]

        insertString='';
        doInsert=1;
        ts = time.time()
        current_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        for col in col_name:

            # Fetch Average , Standarad deviation and count of the column
            sql = "select  AVG("+col+") as Average, STDDEV("+col+") as Standard_Deviation, count("+col+") as Count FROM ApplicationData.raw_data where "+col+" is NOT NULL;"
            row_count_1=cursor.execute(sql)
            if row_count_1>0:
                result = cursor.fetchone()
                current_count=result['Count']

                # Check if data is inserted or not
                if current_count <= last_count:
                    doInsert=0;
                    break
            else:
                doInsert=0;
                break

            # Fetch Median of the column
            sql1 = "SET @rowindex := -1; "
            sql2 = "SELECT AVG(g."+col+") as Median FROM (SELECT @rowindex:=@rowindex + 1 AS rowindex, "+col+" FROM ApplicationData.raw_data ORDER BY "+col+") AS g WHERE g.rowindex IN (FLOOR(@rowindex / 2) , CEIL(@rowindex / 2));"
            cursor.execute(sql1)
            cursor.execute(sql2)
            result1 = cursor.fetchone()

            # To make a string for all the row together and insert in a single query
            if col=='final_value':
                insertString=insertString + "('"+str(current_timestamp)+"','"+str(col)+"',"+str(result['Average'])+","+str(result['Standard_Deviation'])+","+str(result1['Median'])+","+str(result['Count'])+")"
            else:
                insertString=insertString + "('"+str(current_timestamp)+"','"+str(col)+"',"+str(result['Average'])+","+str(result['Standard_Deviation'])+","+str(result1['Median'])+","+str(result['Count'])+"),"


        # Insert all the data in one query only when it is required
        if doInsert==1:
            insert_q = "INSERT into Bhandari.output_data(timestamp, Column_Name, Average, Standard_Deviation, Median, Count) values "+insertString+";"
            cursor.execute(insert_q)
        else:
            print("No New Data Inserted")        
finally:
    connection.commit()
    connection.close()
