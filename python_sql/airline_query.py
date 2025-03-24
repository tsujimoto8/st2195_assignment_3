import sqlite3
import pandas as pd
import os

# set directory
os.chdir('C:/Users/ttuk/Dropbox/study/UoL/ST2195 Programming for Data Science/lecture3')

#check directory
os.getcwd()

try:
    os.remove('airline2.db')
except OSError:
    pass

conn = sqlite3.connect('airline2.db')

airports = pd.read_csv("airports.csv")
carriers = pd.read_csv("carriers.csv")
planes = pd.read_csv("plane_data.csv")
ontime = pd.read_csv("ontime.csv")

# index = False to ensure the DataFrame row index is not written into the SQL tables
airports.to_sql('airports', con = conn, index = False) 
carriers.to_sql('carriers', con = conn, index = False)
planes.to_sql('planes', con = conn, index = False)
ontime.to_sql('ontime', con = conn, index = False)

c = conn.cursor()

q2 = c.execute('''
                SELECT planes.model, AVG(DepDelay) as avg_delay
                FROM ontime, planes
                WHERE (planes.model = '737-230' OR planes.model = 'ERJ 190-100 IGW' OR planes.model = 'A330-223' OR planes.model = '737-282') AND ontime.tailnum = planes.tailnum AND ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
                GROUP BY planes.model
                ORDER BY avg_delay
''').fetchall()

pd.DataFrame(q2)


q3 = c.execute('''
                SELECT airports.city, COUNT(ontime.Dest) as count_dest
                FROM airports JOIN ontime ON airports.iata = ontime.Dest
                WHERE ontime.cancelled = 0 AND airports.city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
                GROUP BY airports.city
                ORDER BY count_dest DESC
''').fetchall()

pd.DataFrame(q3)


q4 = c.execute('''
                SELECT  carriers.Description, SUM(ontime.Cancelled) as sum_cancelled
                FROM carriers JOIN ontime ON carriers.Code = ontime.UniqueCarrier
                WHERE carriers.Description in ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                GROUP BY carriers.Description
                ORDER BY sum_cancelled DESC
''').fetchall()

pd.DataFrame(q4)


q5 = c.execute('''
                SELECT  carriers.Description, CAST(SUM(ontime.Cancelled) AS FLOAT) / CAST(COUNT(ontime.Cancelled) AS FLOAT) as ratio_cancelled
                FROM carriers JOIN ontime ON carriers.Code = ontime.UniqueCarrier
                WHERE carriers.Description in ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                GROUP BY carriers.Description
                ORDER BY ratio_cancelled DESC
''').fetchall()

pd.DataFrame(q5)


conn.close()

