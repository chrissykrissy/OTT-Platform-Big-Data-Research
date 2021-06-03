# OTT-Platform-Big-Data-Research
Topic: SmartStream: Which Service Should I Choose?

We investigated different OTT platform data sets to provide users with insights into each platform to determine which services to subscribe to. Amongst multiple factors affecting online streaming subscriptions, we mainly analyzed age, genre and genome-tags using Spark and Hive.

For full research paper visit : https://docs.google.com/document/d/1C05Z8HRdk2AU7fl46aK2b7WpDHYyiF02pJL9_GSziK0/edit?usp=sharing 

## Profiling
This is the code for profiling the data sets. This mainly checks how many records are in the data sets. We used this code before and after cleaning. 

## Cleaning
This contains the code for cleaning the data sets. The folder contains three different cleaning codes. First, it contains cleaning for all Amazon movies in MapReduce. Second, it contains cleaning original Amazon movies using MapReduce. Lastly, it contains cleaing and combining those two results using Hive.

## Analytics
This contains the code for analytics. All the analytics were done using Spark. There are two different analytics in the folder. First, age/genre analytics using IMDb data. Second, genome-tag analytics using MovieLens data.


If you want more information visit : https://xsyjeon.medium.com/smartstream-which-service-should-i-choose-1e81678bc5b6 
