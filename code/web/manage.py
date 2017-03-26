from flask import Flask, render_template
from flask_script import Manager
from flask_bootstrap import Bootstrap
import MySQLdb
import happybase

app = Flask(__name__)

manager = Manager(app)
bootstrap = Bootstrap(app)

conn = MySQLdb.connect(host = 'ip', port = 3306, user = 'username', passwd = 'password', db = 'dbname')
cur = conn.cursor()

connection = happybase.Connection('ip', port=9090, autoconnect=False)
table = connection.table('iot_device')

@app.route('/')
def index():
    # Get data from mysql
    data = []
    query = "select * from device"
    dbData = cur.execute(query)
    devices = cur.fetchmany(dbData)
    connection.open()
    for device in devices:
        item = {}
        item['id'] = str(device[0])
        item['deviceno'] = str(device[1])
        item['type'] = str(device[2])
        item['imei'] = str(device[3])
        item['ccid'] = str(device[4])
        item['softver'] = str(device[5])
        item['hardver'] = str(device[6])

        row = table.row(bytes(str(device[1])[::-1]))
        item['temp'] = row[b'status:temp']
        item['humidity'] = row[b'status:humidity']
        item['pm25'] = row[b'status:pm25']
        item['mode'] = row[b'status:mode']
        data.append(item)

    connection.close()
    cur.close()
    conn.commit()
    conn.close()

    return render_template('index.html', data = data)

if __name__ == '__main__':
    app.run(debug=True)
