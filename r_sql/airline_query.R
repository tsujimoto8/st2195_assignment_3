#install.packages("RSQLite")
#install.packages("bit")
library(DBI)
library(dplyr)

getwd()
setwd("C:/Users/ttuk/Dropbox/study/UoL/ST2195 Programming for Data Science/lecture3")

if (file.exists("airline2")) 
  file.remove("airline2")

conn <- dbConnect(RSQLite::SQLite(), "airline2")

airports <- read.csv("airports.csv", header = TRUE)
carriers <- read.csv("carriers.csv", header = TRUE)
planes <- read.csv("plane_data.csv", header = TRUE)
ontime <- read.csv("ontime.csv", header = TRUE)

dbWriteTable(conn, "airports", airports)
dbWriteTable(conn, "carriers", carriers)
dbWriteTable(conn, "planes", planes)
dbWriteTable(conn, "ontime", ontime)

# Convert column names to lowercase
colnames(ontime) <- tolower(colnames(ontime))

colnames(ontime)

# list all tables
dbListTables(conn)

# Question 2
q2_1 <- dbGetQuery(conn, 
                   "SELECT planes.model, AVG(DepDelay) as avg_delay
                    FROM ontime, planes
                    WHERE (planes.model = '737-230' OR planes.model = 'ERJ 190-100 IGW' OR planes.model = 'A330-223' OR planes.model = '737-282') AND ontime.tailnum = planes.tailnum AND ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
                    GROUP BY planes.model
                    ORDER BY avg_delay")
q2_1

q2_2 <- ontime %>% 
  inner_join(planes, by = "tailnum", suffix = c(".ontime", ".planes")) %>%
  filter(cancelled == 0 & diverted == 0 & depdelay > 0 & (model == '737-230' | model == 'ERJ 190-100 IGW' | model == 'A330-223' | model == '737-282')) %>%
  group_by(model) %>%
  summarize(avg_delay = mean(depdelay)) %>%
  arrange(avg_delay) 
q2_2

# Question 3
q3_1 <- dbGetQuery(conn, 
                   "SELECT airports.city, COUNT(ontime.Dest) as count_dest
                    FROM airports JOIN ontime ON airports.iata = ontime.Dest
                    WHERE ontime.cancelled = 0 AND airports.city IN ('Chicago', 'Atlanta', 'New York', 'Houston')
                    GROUP BY airports.city
                    ORDER BY count_dest DESC")
q3_1

q3_2 <- ontime %>% 
  inner_join(airports, by = c("dest" = "iata")) %>%
  filter(cancelled == 0 & city %in% c('Chicago', 'Atlanta', 'New York', 'Houston')) %>%
  group_by(city) %>%
  summarize(count_dest = n()) %>%
  arrange(desc(count_dest))
q3_2


# Question 4
q4_1 <- dbGetQuery(conn, 
                   "SELECT  carriers.Description, SUM(ontime.Cancelled) as sum_cancelled
                    FROM carriers JOIN ontime ON carriers.Code = ontime.UniqueCarrier
                    WHERE carriers.Description in ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                    GROUP BY carriers.Description
                    ORDER BY sum_cancelled DESC")
q4_1

q4_2 <- ontime %>% 
  inner_join(carriers, by = c("uniquecarrier" = "Code")) %>%
  filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(sum_cancelled = sum(cancelled)) %>%
  arrange(desc(sum_cancelled)) 
q4_2


# Question 5
q5_1 <- dbGetQuery(conn, 
                   "SELECT  carriers.Description, CAST(SUM(ontime.Cancelled) AS FLOAT) / CAST(COUNT(ontime.Cancelled) AS FLOAT) as ratio_cancelled
                    FROM carriers JOIN ontime ON carriers.Code = ontime.UniqueCarrier
                    WHERE carriers.Description in ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
                    GROUP BY carriers.Description
                    ORDER BY ratio_cancelled DESC")
q5_1

q5_2 <- ontime %>% 
  inner_join(carriers, by = c("uniquecarrier" = "Code")) %>%
  filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(ratio_cancelled = sum(cancelled)/n()) %>%
  arrange(desc(ratio_cancelled)) 
q5_2

# Alternatively,we can just use mean function as below.
q5_3 <- ontime %>% 
  inner_join(carriers, by = c("uniquecarrier" = "Code")) %>%
  filter(Description %in% c('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')) %>%
  group_by(Description) %>%
  summarize(ratio_cancelled = mean(cancelled)) %>%
  arrange(desc(ratio_cancelled)) 
q5_3









