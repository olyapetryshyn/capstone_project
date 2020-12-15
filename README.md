# capstone_project

#### This project is implemented for educational purposes to generalize the knowledge gained during Big Data retraining program.

### Solution Documentation

#### Task 1. Building Purchases Attribution Projection
##### Task 1.1
Using standard DataFrame API implemented a function with custom creation of sessionId field based on event types. Cleared attributes field to be in the same format as the rest of the dataset. Retrieved needed columns from it. 

##### Task 1.2
Basically did the same thing here, but using custom udfs (user defined functions) to flag the start of each session and to replace some symbols in a string of attributes column. In general using custom udfs is not recommended when you can use standard pyspark functions as Spark doesn't know how to optimize them, so we miss out on many cool optimization that Spark does for us. 

#### Task 2. Calculating Marketing Campaigns And Channels Statistics
##### Task 2.1
Using both plain SQL and DataFrame API got the names of top marketing campaigns hat bring the biggest revenue.

##### Task 2.2
Using both plain SQL and DataFrame API got the names of the channels that drives the highest amount of unique sessions in each campaign.

#### Implemented by:
- @olya_petryshyn
