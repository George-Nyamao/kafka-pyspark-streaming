import sys
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pymysql

#connect to mysql database
con = pymysql.connect(host='localhost',user='morara',passwd='d3barl',db='capstone')
cursor = con.cursor()
cursor.execute("select date, close, open from msstock order by date")

result = cursor.fetchall()

date  = []
close = []
open = []

for record in result:
        date.append(record[0])
        close.append(record[1])
        open.append(record[2])

plt.plot(date,close,'ro')
plt.show()