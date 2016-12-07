Stock Price Analysis 1 - Hadoop MapReduce
-----------------------------------------
The program calculates a few statistics on stocks/tickers

1. There are 3 operations to be performed on the data - Min, Max and Average of high, low, close fields between the given start and end dates
2. Input file a csv file (comma separated). The fields are ticker, date, open, high, low, close, volume, ex-dividend, split_ratio, adj_open, adj_high, adj_low, adj_close, adj_volume
3. The program accepts 6 positional arguments - start date, end date, the operation to be performed, field on which the operation should be performed, input path and output path

Sample commands to execute
$ hadoop jar AnalyzeStock.jar 10/01/1990 12/01/2015 avg low /user/cloudera/stockdata.csv /user/cloudera/stockoutput
$ hadoop jar AnalyzeStock.jar 10/01/1990 12/01/2015 min close /user/cloudera/stockdata.csv /user/cloudera/stockoutput
$ hadoop jar AnalyzeStock.jar 10/01/1990 12/01/2015 max high /user/cloudera/stockdata.csv /user/cloudera/stockoutput

Make sure the output folder is empty

Stock Price Analysis 2
----------------------
The program finds the stock/ticker with the highest fluctuation (difference between high and low prices) for every year

1. The program accepts 2 positional arguments - input path and output path.
2. Output format - YEAR TICKER LOW HIGH FLUCTUATION

Sample command to execute
$ hadoop jar AnalyzeStockAdvanced.jar /user/cloudera/stockdata.csv /user/cloudera/stockoutput1