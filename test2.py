from flask import Flask, render_template
import psycopg2

app = Flask(__name__, template_folder= "C:/Users/krish/PycharmProjects/app")

@app.route("/", methods=['post', 'get'])
def viewall():
    conn = None
    try:
        #connect to posgresql database
        conn = psycopg2.connect(database = "test", user = "postgres", password = "Krishna_1992", host = "127.0.0.1", port = "5432")
        print('connection successfull')
        cur = conn.cursor()
        cur.execute("select * from student_1")
        result = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
         render_template("fail.html")
    finally:
      conn.close()
    return render_template("webapp.html",rows = result)



if __name__ =='__main__':
    app.run(debug=True)