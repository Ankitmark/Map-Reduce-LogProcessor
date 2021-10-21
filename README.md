# Homework 2

### Ankit Kumar Singh
### 651288872
### Email: asing200@uic.edu

## Instruction to run this Project:

1. Clone the repository using the following command: git clone ``` https://github.com/Ankitmark/LogFileGenerator.git ```
2. Now, navigate to the directory 'LogFileGenerator' and run the following command: ``` sbt clean compile assembly ```
3. A fat far file will be created in 'LogFileGenerator/target/scala-version'.
4. This jar file has to be copied to the Hadoop File System. The following instructions are for those who have installed hortonworks and are using windows.
5. Start hortonworks in your preferred hypervisor. Visit this link: https://www.cloudera.com/tutorials/learning-the-ropes-of-the-hdp-sandbox.html to setup your id and password.
6. Once started, open your browser and type the link provided in the VM for your hypervisor.
7. Now, open the Ambari Dashboard and wait for the required functionalities to start.
8. Now, using your preferred transfer software, (like WinSCP) transfer the jar file to the VM.
9. The dataset has to be added to Ambari. Assuming that you are logged in to Ambari from terminal.
10. Now, to make a directory for the dataset, use the following command: ``` hdfs dfs -mkdir <path to dataset> ```
11. To put the dataset in the directory, use the following command: ``` hdfs dfs -put <path to dataset>/ ```. Now to execute the jar file, use the following command: ``` hadoop jar <path to JAR file> <class name> <path to dataset> <output path> ```
