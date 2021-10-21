
## Overview

The goal of this project is to solve a distributed computational problem using cloud computing technologies. In this we create a distributed program for parallel processing of the log files that are generated. Each entry in the log dataset describes a fictitios log message, which contains the time of the entry, the logging context name, the message level (i.e., INFO, WARN, DEBUG or ERROR), the name of the logging module and the message itself. Below is the sample log data:

```
17:02:52.558 [main] INFO  GenerateLogData$ - Log data generator started...
17:02:52.804 [main] WARN  HelperUtils.Parameters$ - Max count 50000 is used to create records instead of timeouts
17:02:53.194 [scala-execution-context-global-17] ERROR HelperUtils.Parameters$ - s%]s,+2k|D}K7b/XCwG&@7HDPR8z
17:02:53.510 [scala-execution-context-global-17] INFO  HelperUtils.Parameters$ - ;kNI&V%v<c#eSDK@lPY(
17:02:53.823 [scala-execution-context-global-17] INFO  HelperUtils.Parameters$ - l9]|92!uHUQ/IVczbg1L8rX5fF6gcf3~(;.Uz%K*5jTUd08
17:02:54.135 [scala-execution-context-global-17] INFO  HelperUtils.Parameters$ - RA/MedXk>#SsSJM":X08;SveB{irOdBd*[C4#16XR[N6ldH(
17:02:54.448 [scala-execution-context-global-17] WARN  HelperUtils.Parameters$ - _Zmsa_4@mzI%}d
```

## Map-Reduce Tasks Implementation

### 1. Compute a spreadsheet or an CSV file that shows the distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types.

### Mapper class: TokenizerMapper

In this task the mapper take the log as input produce the time interval as key which is one minute and the rest of the log as value. In this processes the Mapper take the timestamp from the log and add one minute to it and create this start time and end time as time interval. Then the mapper strip out the rest of the log and combine it with the time interval to make the key and value pair where the time interval is the key and the message is the value.

### Reducer class: IntSumReader

Take the time interval as key and finds the distribution of different types of messages across time intervals. In this processes the reducer takes the time interval as key and iterate over all the values of the key that is all the messages paired with this key and compute the count of the INFO, WARN, DEBUG and ERROR messages in this interval. After this the reducer produce the string of the message type with its corresponding counts as value.

The output snippet looks as follows:
```

17:02 to 17:03,WARN: 1 DEBUG: 0 INFO: 1 ERROR: 0
17:03 to 17:04,WARN: 1 DEBUG: 2 INFO: 11 ERROR: 3
17:04 to 17:05,WARN: 3 DEBUG: 4 INFO: 9 ERROR: 0
17:05 to 17:06,WARN: 2 DEBUG: 3 INFO: 10 ERROR: 1
17:06 to 17:07,WARN: 2 DEBUG: 1 INFO: 16 ERROR: 0
17:07 to 17:08,WARN: 2 DEBUG: 4 INFO: 6 ERROR: 2
17:08 to 17:09,WARN: 1 DEBUG: 3 INFO: 1 ERROR: 0

```

### 2. Compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected regex pattern string instances.

In this task two map-reduce jobs are used to first get the error counts and then to sort the counts.

### First Mapper class: TokenizerMapper

In this task the mapper take the log as input produce the time interval as key which is one minute and the rest of the log as value. In this processes the Mapper take the timestamp from the log and add one minute to it and create this start time and end time as time interval. Then the mapper strip out the rest of the log and combine it with the time interval to make the key and value pair where the time interval is the key and the message is the value.

### First Reducer class: Task2Reducer

Take the time interval as key and finds the distribution of different ERROR messages across time intervals. In this processes the reducer takes the time interval as key and iterate over all the values of the key that is all the messages paired with this key and compute the count of the ERROR messages in this interval. After this the reducer produce the string of the message type with its corresponding counts as value.

### Second Mapper class: keyValueSwapper

This Mapper takes the input from first reducer and swap the key and the value of the result from the first reducer so that we can sort on the key.

### Second Reducer class: secondReducer

This Reducer again swaps the key and value from the keyValueSwapper mapper after the sorting of the ERROR count in descending order.

### class: DecreasingComparator

This class sorts the result from the keyValueSwapper mapper class where the keys are the ERROR counts.


The output snippet looks as follows:
```
17:03 to 17:04 ERROR Count = ,3
17:07 to 17:08 ERROR Count = ,2
17:05 to 17:06 ERROR Count = ,1
17:08 to 17:09 ERROR Count = ,0

```

### 3. Compute for each message type you the number of the generated log messages.

### Mapper class: TokenizerMapper

Take the log as input produce the message type as key and assign one as value.

### Reducer class: IntSumReader

Take message type as key and produce the sum of the number of message for each message type as value. This Rducer produce the aggregate sum of the values for each log type.

The output snippet looks as follows:
```
DEBUG,4595
ERROR,95
INFO,870
WARN,118
```

### 4. Produce the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.

### Mapper class: Task4Mapper

Mapper takes the log as input and produce logtype as key and the attached message as the value. In this processes the mapper strip out the log type from the log and the message and produce the key as log type and value as its corresponding message.

### Reducer class: Task4Reducer

Reducer takes the log type as the key and finds the max length of the message associated to that log type. In this the reducer iterate over all the messages for a log type and find the message that contain the highest number of characters and assign it to the value. 

The output snippet looks as follows:
```
DEBUG,86
ERROR,64
INFO,94
WARN,82

```

## Deployment and Running on AWS EMR:

Kindly find below the uploaded video to Youtube which demonstrates my deployment and running the jobs on AWS.

Link: https://youtu.be/2mWMoNcaAC0
