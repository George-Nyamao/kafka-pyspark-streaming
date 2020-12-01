import sys
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pymysql
plt.style.use('fivethirtyeight')

fig = plt.figure()
ax = fig.add_subplot(1,1,1)
date = []
close = []

def plot_stock(i, date, close):
        #connect to mysql database
        con = pymysql.connect(host='localhost',user='morara',passwd='d3barl',db='stocks')
        cursor = con.cursor()
        cursor.execute("select distinct date, close from wmtstock order by date")

        result = cursor.fetchall()

        date  = []
        close = []

        for record in result:
                date.append(record[0])
                close.append(record[1])

        plt.cla()
        plt.plot(date, close, 'b-')
        plt.title('Walmart Stock Data - closing price', fontsize=20)
        plt.xlabel(' Date ', fontsize=14)
        plt.xticks(rotation=90)
        plt.ylabel('Close Price', fontsize=14)

ani = FuncAnimation(fig, plot_stock, fargs=(date, close), interval=500)
plt.show()