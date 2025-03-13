import psycopg2
import pandas as pd

# Connection parameters, yours will be different
# Permanently changes the pandas settings
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', -1)
param_dis = {
    "host": "127.0.0.1",
    "database": "test",
    "user": "postgres",
    "password": "xxxxx"
}


def connect(params_dis):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dis)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


def execute_sql_query(connection, sql):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
            print("Query Executed..")
    except:
        print("Error executing sql")
        raise


def execute_sql_bulk_records(connection, sql, records_list):
    try:
        with connection.cursor() as cursor:
            print(sql)
            print(records_list)
            cursor.executemany(sql, records_list)
            connection.commit()
            print("Record count : ", cursor.rowcount)
            print("Query Executed..")
    except:
        print("Error executing sql")
        raise


def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tuples
    tuples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tuples, columns=column_names)
    return df


def action_to_perform(action) :
    if action == 'Show':
        print("Type Table Name to Show")
        tablename = input()

        if tablename == 'faculty':
            column_names = ["department", "salary", "Faculty Name", "Faculty Id", "address", "designation"]
        elif tablename == 'student_1':
            column_names = ["sid", "Student Name", "address", "major", "gpa", "age", "gender"]
        elif tablename == 'course':
            column_names = ["courseid", "coursename"]
        else:
            print('Provide Proper table Name')
            exit(1)

        sql_query = "select * from " + tablename;
        # print(sql_query)
        df = postgresql_to_dataframe(conn, sql_query, column_names)
        print(df)
    elif action == 'Add':
        print("Type Table Name to Add")
        tablename = input()
        if tablename == 'student_1':
            insert_sql = """INSERT INTO student_1(sid, "Student Name", address, major, gpa, age, gender) VALUES (%s,%s,%s,%s,%s,%s,%s);"""
            print("Provide the value's with comma and '|' separated ")
            insert_list = []
            all_record = input().split('|');
            all_record_list = []
            for per_record in all_record:
                record_tup = tuple(per_record.split(','))
                all_record_list.append(record_tup)
            print("all_record_list - ", all_record_list)
            # insert_list.append(input_insert)
            execute_sql_bulk_records(conn, insert_sql, all_record_list)
    elif action == 'Update':
        print("Type Table Name to Update with Sid")
        tablename = input()
        if tablename == 'student_1':
            update_sql = """Update student_1 set "Student Name" = %s, address = %s , major = %s, gpa = %s , age = %s , gender = %s where sid = %s
            """
            print("Provide the value's with comma and '|' separated ")
            insert_list = []
            all_record = input().split('|');
            all_record_list = []
            for per_record in all_record:
                record_tup = tuple(per_record.split(','))
                all_record_list.append(record_tup)
            print("all_record_list - ", all_record_list)
            # insert_list.append(input_insert)
            execute_sql_bulk_records(conn, update_sql, all_record_list)
    elif action == 'Remove':
        print("Type Table Name to Remove")
        tablename = input()
        print("condition..")
        condition = input()
        delete_sql="Delete from "+tablename + " where "+ condition
        execute_sql_query(conn,delete_sql)
    elif action == 'search':
        print("Type Table Name to search")
        tablename = input()
        if tablename == 'course':
            print("Type coursename to search")
            coursename = input()
            search_sql = "select courseid from " + tablename + " where coursename  = '" +coursename+ "';"
            print("columnname to be given")
            column_names = input().split(',')
            df = postgresql_to_dataframe(conn, search_sql, column_names)
            print(df)

        elif tablename == 'faculty':
            print("Type the major to search Faculty Name")
            major = input()
            search_sql = "select facultyname from " + tablename + " where department = '" +major+ "';"
            print("columnname to be given")
            column_names = input().split(',')
            df=postgresql_to_dataframe(conn, search_sql, column_names)
            print(df)
    elif action == 'count':
            print("Type Table Name to count")
            tablename = input()
            if tablename == 'course':
                print("Type the coursename")
                coursename = input()
                count_sql = "select count(*)  from " + tablename + " where coursename  = '" +coursename+ "';"
                column_names = input().split(',')
                df = postgresql_to_dataframe(conn, count_sql, column_names)
                print(df)
            elif tablename == 'student_1':
                print("Type the major")
                major = input()
                count_sql = "select count(*)  from " + tablename + " where major  = '" +major+ "';"
                column_names = input().split(',')
                df = postgresql_to_dataframe(conn, count_sql, column_names)
                print(df)
    elif action == 'salary':
                print("Type Table Name to count")
                tablename = input()
                if tablename == 'faculty':
                    print("Type Department Name to get facultycount salary > 2000 ")
                    dprt = input()
                    salary = "select count(*)  from " + tablename + " where salary > 2000 and department = '" +dprt+ "';"
                    print("provide columnname ")
                    column_names = input().split(',')
                    df = postgresql_to_dataframe(conn, salary, column_names)
                    print(df)
    elif action == 'salaryabove4000':
                    print("Type Table Name to count")
                    tablename = input()
                    if tablename == 'faculty':
                        salary = "select facultyname from faculty where salary in ( select salary from faculty where salary > '4000');"
                        print("Type the columnname ")
                        column_names = input().split(',')
                        df = postgresql_to_dataframe(conn, salary, column_names)
                        print(df)
    elif action == 'max':
                print("Type Table Name to find max salary")
                tablename = input()
                if tablename == 'faculty':
                    maximum = "select department,max (salary) from " + tablename + " where department in ('CS','EEE','ECE','phy','Math') group by department"
                    # maximum = "select max (salary) from " + tablename + " where department = 'EEE'"
                    # maximum = "select max (salary) from " + tablename + " where department = 'ECE'"
                    # maximum = "select max (salary) from " + tablename + " where department = 'phy'"
                    # maximum = "select max (salary) from " + tablename + " where department = 'Math'"
                    print("sql ",maximum)
                    print("departname with salary ")
                    column_names = input().split(',')
                    df = postgresql_to_dataframe(conn, maximum, column_names)
                    print(df)
    elif action == 'facultycountwithid':
                    print("Type the table name to find facultycount")
                    tablename = input()
                    if tablename == 'faculty':
                        print("Type the departmentname")
                        count_sql = "select count ('Faculty Id') from faculty where (department) in (select department from faculty where department = 'CS')"
                        column_names = input().split(',')
                        df = postgresql_to_dataframe(conn, count_sql, column_names)
                        print(df)
    elif action == 'orderby':
                    print("Type the table name to orderby")
                    tablename = input()
                    if tablename == 'student_1':
                        print("provide the column names of student_1")
                        group_sql = "(select * from student_1 order by sid)"
                        column_names = ["sid", "Student Name", "address", "major", "gpa", "age", "gender"]
                        column_names = input().split(',')
                        df = postgresql_to_dataframe(conn, group_sql, column_names)
                        print(df)
    else:
        print("Give proper Action command")
        exit()


#main
conn = connect(param_dis)

print("Type Action Start or Close")
action = input()
while action != 'Close':
    print("Type Action to be performed against DB..i.e 'Show' or 'Remove' or 'Update' or 'Add' or 'search' or count or 'salary' or 'max'")
    action = input()
    action_to_perform(action)
    if action == 'Close':
        break
    continue
print(" Action is , ",action)

