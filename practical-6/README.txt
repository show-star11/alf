Big Data Practical 6: Fraud Detection with Apache Flink (KeyedProcessFunction)
==============================================================================

Name: Arvind Yadav
Roll no: 31011123065
Class: LYCS

Aim
---

Flink State Lab: Implement a Flink application that detects "Fraudulent Transactions" (two large
transactions within 1 minute) using KeyedProcessFunction.

Theory
------

Apache Flink. Apache Flink is an open-source framework for processing large amounts of data in real
time. It processes each record as soon as it arrives and can remember information between records
(state).

  Concept                Meaning
  ---------------------- -------------------------------------------------------------------------------------------------------
  DataStream             A continuous flow of data
  map()                  Transforms each record
  filter()               Keeps only the records that match a condition
  keyBy()                Groups the stream by a key, here the customer ID, so all records of one customer go to the same place
  KeyedProcessFunction   Applies custom logic to every record of each key, with access to that key's state
  ValueState             Stores one value per key that is remembered between records
  Checkpointing          Saves the state regularly so the job can recover after a failure

How the fraud check works.

1.  The stream of transactions is grouped by customerId with keyBy().
2.  For each customer, FraudDetector remembers the last large transaction (₹10,000 or more) in a
    ValueState.
3.  When a new large transaction arrives, it is compared with the remembered one. If both are large
    and the time gap is 60 seconds or less, a fraud alert is produced.
4.  The new transaction then replaces the remembered one.

Because the state is kept per key, the transactions of one customer never affect the check for
another customer.

Where Flink is used. Banking (fraud detection, payment monitoring), e-commerce (order tracking,
customer activity), healthcare (live monitoring data), IoT (data from connected devices) and telecom
(network events).

Sample data and expected result
-------------------------------

Times are seconds after 10:00:00 on 19 Oct 2026. Large means ₹10,000 or more.

  Time    Customer   Amount   What happens
  ------- ---------- -------- --------------------------------------------
  10 s    C101       15,000   Large; remembered for C101
  20 s    C202       5,000    Small; ignored
  45 s    C101       20,000   Large; 35 s after ₹15,000, so FRAUD ALERT
  50 s    C202       12,000   Large; remembered for C202
  55 s    C101       3,000    Small; ignored
  90 s    C101       18,000   Large; 45 s after ₹20,000, so FRAUD ALERT
  130 s   C202       14,000   Large, but 80 s after ₹12,000, so no alert

So the program prints exactly 2 alerts, both for C101.

Files
-----

  File                                   Purpose
  -------------------------------------- -----------------------------------------------------
  pom.xml                                Maven project file: Java 17, Flink 1.19.2 libraries
  src/main/java/FraudDetectionJob.java   The Flink program

The folder structure must be exactly:

    practical-6
    ├── pom.xml
    └── src
        └── main
            └── java
                └── FraudDetectionJob.java

Maven only compiles Java files inside src\main\java, and the file name must match the class name
FraudDetectionJob, including capital letters.

Requirements (Windows)
----------------------

1.  Java JDK 17
2.  Apache Maven
3.  VS Code with the Extension Pack for Java (by Microsoft)
4.  Internet connection for the first build (Maven downloads the Flink libraries)

Steps to run (Windows PowerShell)
---------------------------------

If a step does not give the expected result, the error number in brackets points to the fix in
Errors on Windows and how to fix them.

Step 1. Install Java JDK 17

Download JDK 17 from https://adoptium.net (Temurin 17) and install it. Choose Set JAVA_HOME variable
and Add to PATH in the installer. Check in a new terminal:

    java -version

Expected: a line starting with openjdk version "17. (Error 1.)

Step 2. Install Apache Maven

Maven downloads the Flink libraries and builds the project.

1.  Open https://maven.apache.org/download.cgi.
2.  In the table, download the Binary zip archive (apache-maven-3.x.x-bin.zip).
3.  Extract it to C:\Tools, so that the folder C:\Tools\apache-maven-3.x.x\bin exists.
4.  Add that bin folder to PATH. Search Windows for Edit the system environment variables, click
    Environment Variables, select Path under User variables, click Edit > New, paste the bin folder
    path and click OK on every window.
5.  Close VS Code and every terminal, open them again and check:
        mvn -version

    Expected: Apache Maven 3.x.x and the Java version. (Error 2.)

Step 3. Install the Java extensions in VS Code

1.  Open VS Code and click the Extensions icon in the left sidebar.
2.  Search for Extension Pack for Java and install the one published by Microsoft.

It adds Java language support, debugging and Maven integration.

Step 4. Get the project

    git clone https://github.com/show-star11/alf.git
    cd alf\practical-6

Open the practical-6 folder in VS Code with File > Open Folder. To create the project by hand
instead, make the folders and files exactly as shown under Files above and paste the code into them.

Step 5. Download the Flink libraries and compile

    mvn clean compile

The first run downloads the Flink libraries, which takes a few minutes. Expected at the end:
BUILD SUCCESS. (Errors 3 and 4.)

Step 6. Save the list of Flink libraries

    mvn dependency:build-classpath "-Dmdep.outputFile=classpath.txt"

This writes the location of every Flink library into classpath.txt, so Java can find them in the
next step. Expected: BUILD SUCCESS.

Step 7. Run the program

    $cp = Get-Content .\classpath.txt
    java -cp "target\classes;$cp" FraudDetectionJob

Expected (see Output). (Errors 5 and 6.)

Code explanation
----------------

pom.xml

  Part                           Meaning
  ------------------------------ -----------------------------------------------------------------------
  groupId, artifactId, version   The name and version of this project
  maven.compiler.release 17      Compile for Java 17
  flink.version 1.19.2           The Flink version used by both dependencies
  flink-streaming-java           The Flink DataStream API: streams, keyBy, KeyedProcessFunction, state
  flink-clients                  Lets the program run Flink locally on this computer
  maven-compiler-plugin          The plugin that compiles the Java code

FraudDetectionJob.java

1.  Transaction holds one bank transaction: customerId, amount and timestamp in milliseconds. It has
    public fields and an empty constructor, so Flink can treat it as a POJO and store it in state.
2.  FraudAlert holds one alert. Its toString() builds the line that is printed, and converts the gap
    from milliseconds to seconds.
3.  FraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert>: the key is a String
    (the customer ID), the input is Transaction and the output is FraudAlert.
4.  open() runs once at start-up. It creates a ValueState<Transaction> named
    previous-large-transaction, which Flink keeps separately for every customer.
5.  processElement() runs for every transaction:
    -   Transactions below ₹10,000 are ignored with return.
    -   previousTransaction.value() reads the last large transaction of this customer.
    -   If there is one, the gap is tx.timestamp - previous.timestamp. If the gap is between 0 and
        60,000 ms (1 minute), an alert is sent with out.collect(...).
    -   previousTransaction.update(tx) stores the current transaction for the next check.
6.  main():
    -   StreamExecutionEnvironment.getExecutionEnvironment() creates the Flink environment; on a
        normal computer it runs Flink locally.
    -   base is 10:00:00 on 19 Oct 2026 in milliseconds, and each transaction's time is base plus a
        number of milliseconds.
    -   env.fromData(...) turns the 7 sample transactions into a DataStream.
    -   .keyBy(tx -> tx.customerId) groups the stream by customer, .process(new FraudDetector())
        applies the fraud check, and .print() prints the alerts.
    -   env.execute("Fraud Detection Job") starts the job. Nothing runs until this line.

Output
------

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
    7> FRAUD ALERT: Customer=C101, Previous=₹15000.0, Current=₹20000.0, Gap=35 seconds
    7> FRAUD ALERT: Customer=C101, Previous=₹20000.0, Current=₹18000.0, Gap=45 seconds

-   The SLF4J lines only mean that no logging library is set up. They are normal and can be ignored.
-   7> is the number of the Flink task that printed the line. It can be a different number on
    another computer.
-   The Windows terminal may show ? instead of ₹ (Error 7).

Observations
------------

-   Only customer C101 gets alerts: two pairs of large transactions within 1 minute (35 s and 45 s
    apart).
-   The ₹3,000 transaction of C101 is ignored, so it does not break the pair of ₹20,000 and ₹18,000.
-   C202's large transactions are 80 seconds apart, more than 1 minute, so no alert is produced.
-   The state is kept per customer, so C101's transactions never affect the check for C202.

Errors on Windows and how to fix them
-------------------------------------
Error 1: 'java' is not recognized, or the wrong Java version

Cause: Java is not installed, or not on PATH.

1.  Install JDK 17 from https://adoptium.net and choose Set JAVA_HOME variable and Add to PATH.
2.  Close VS Code and every terminal, open them again and run java -version.

Error 2: 'mvn' is not recognized

Cause: Maven's bin folder is not on PATH, or the terminal was opened before PATH was changed.

1.  Check that mvn.cmd exists in the folder you added, for example C:\Tools\apache-maven-3.x.x\bin.
    The path must end in \bin.
2.  Close VS Code and every terminal, open them again and run mvn -version.
3.  To use Maven in the current terminal only:
        $env:PATH += ";C:\Tools\apache-maven-3.x.x\bin"

    Replace 3.x.x with the version in your folder name.

Error 3: The JAVA_HOME environment variable is not defined correctly

Cause: Maven cannot find Java.

1.  Find the JDK folder, usually under C:\Program Files\Eclipse Adoptium\.
2.  Set JAVA_HOME to that folder (without \bin), for example:
        setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
3.  Close and reopen the terminal and run mvn -version.

Error 4: Could not resolve dependencies or Could not transfer artifact

Cause: Maven could not download the Flink libraries, usually because the internet connection
dropped.

1.  Check the connection and run mvn clean compile again.
2.  If it keeps failing, delete the half-downloaded files and try again:
        Remove-Item -Recurse -Force "$env:USERPROFILE\.m2\repository\org\apache\flink"
        mvn clean compile

Error 5: Error: Could not find or load main class FraudDetectionJob

Cause: the program was not compiled, the file is in the wrong folder, or the command was run from
the wrong folder.

1.  Run dir and check that pom.xml is in the current folder.
2.  Check that the file is src\main\java\FraudDetectionJob.java, with the same capital letters as
    the class name.
3.  Run mvn clean compile again and check that target\classes\FraudDetectionJob.class exists.
4.  Run Steps 6 and 7 again.

Error 6: NoClassDefFoundError: org/apache/flink/...

Cause: Java cannot find the Flink libraries, because classpath.txt is missing or $cp is empty.

1.  Run Step 6 again and check that classpath.txt exists in the project folder.
2.  Run both lines of Step 7 in the same terminal; $cp is lost when the terminal is closed.

Error 7: ? instead of ₹ in the output

Cause: the Windows terminal does not use UTF-8, so it cannot show the ₹ sign. The program itself is
correct.

1.  Run these before Step 7:
        chcp 65001
        [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
2.  Run the program with UTF-8 output:
        java "-Dstdout.encoding=UTF-8" "-Dsun.stdout.encoding=UTF-8" -cp "target\classes;$cp" FraudDetectionJob

Error 8: InaccessibleObjectException or module java.base does not "opens" ...

Cause: newer Java versions block some internal access that Flink uses.

1.  Use JDK 17, the version this practical is set up for.
2.  If it still appears, run with the access opened:
        java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED -cp "target\classes;$cp" FraudDetectionJob

Conclusion
----------

A Flink application was built with Maven that groups bank transactions by customer with keyBy() and
checks them with a KeyedProcessFunction. A ValueState stores the last large transaction of each
customer, and an alert is produced when two transactions of ₹10,000 or more come within 1 minute.
The program found the two fraudulent pairs of customer C101 and correctly ignored the small
transactions and C202's transactions that were more than 1 minute apart.
