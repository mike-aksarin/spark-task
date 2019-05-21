# Spark Window Example 

This project contains following spark applications:
* [SessionAggregateApp](src/main/scala/SessionAggregateApp.scala), 
helper runner script is [session-aggregate-app.sh](session-aggregate-app.sh).
Enriches  events with session fields. Implemented with window aggregation functions.
* [SessionSqlApp](src/main/scala/SessionSqlApp.scala), 
helper runner script is [session-sql-app.sh](session-sql-app.sh). 
Enriches  events with session fields. Implemented with sql window functions.
* [StatisticsSqlApp](src/main/scala/StatisticsSqlApp.scala), 
helper runner script is [statistics-sql-app.sh](statistics-sql-app.sh). 
Calculates some statistics on event sessions generated by any of the former two tasks. Requires one of the former tasks to be executed before this.
* [TopProductsApp](src/main/scala/TopProductsApp.scala), 
helper runner script is [top-products-app.sh](top-products-app.sh). 
Calculate top products by session duration.

All the applications take exactly two parameters:
* the first parameter is a file path for the input data;
* the second parameter is a directory name for the output data.

Each application has a helper shell script for running.
 
## Events Input 
 
 [SessionAggregateApp](src/main/scala/SessionAggregateApp.scala), 
 [SessionSqlApp](src/main/scala/SessionSqlApp.scala) and 
 [TopProductsApp](src/main/scala/TopProductsApp.scala) 
 needs event-typed output in CSV format. Helper shell scripts use [data/example.csv](data/example.csv) file as an input for them. To provide different dataset these shell scripts should be updated accordingly. 

Input file should have the following format:
```
category,product,userId,eventTime,eventType
```
It should also contain a header line. 
See the example at [data/example.csv](data/example.csv).

## Sessions Input

 [StatisticsSqlApp](src/main/scala/StatisticsSqlApp.scala) 
  uses results of [SessionAggregateApp](src/main/scala/SessionAggregateApp.scala) or [SessionSqlApp](src/main/scala/SessionSqlApp.scala) as an input and should be executed after one of these applications.

# Application Business Logic

*Domain:* say we have an ecommerce site with products divided into categories like toys, electronics etc. We receive events like product was seen (impression), product page was opened, product was purchased etc. 

### Task #1
Enrich incoming data with user sessions. *Definition of a session:* for each user, it contains consecutive events that belong to a single category  and are not more than 5 minutes away from each other. Output lines should end like this:
```
 …, sessionId, sessionStartTime, sessionEndTime  
```

Implement it using:
 * Sql window functions.
 *(Solved by [SessionSqlApp](src/main/scala/SessionSqlApp.scala))*
 * Spark aggregator.
 *(Solved by [SessionAggregateApp](src/main/scala/SessionAggregateApp.scala))*

### Task #2
Compute the following statistics:
* For each category find median session duration. 
*(Solved by [StatisticsSqlApp](src/main/scala/StatisticsSqlApp.scala))*
* For each category find # of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins.
*(Solved by [StatisticsSqlApp](src/main/scala/StatisticsSqlApp.scala))*
* For each category find top 10 products ranked by time spent by users on product pages — this may require different type of sessions. For this particular task, session lasts until the user is looking at particular product. When particular user switches to another product the new session starts.
*(Solved by [TopProductsApp](src/main/scala/TopProductsApp.scala))*

### General notes
Ideally tasks should be implemented using pure SQL on top of Spark DataFrame API.


