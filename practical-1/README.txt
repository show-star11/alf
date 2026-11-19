Big Data Practical 1: Spark Optimization (Data Skew and Shuffle Bottlenecks)
============================================================================

Name: Arvind Yadav
Roll no: 31011123065
Class: LYCS

Aim
---

Implement a Spark job and use the Spark UI to identify and fix data skew and "shuffle" bottlenecks.

Theory
------

Shuffle. Spark splits data into partitions and runs one task per partition. Operations such as
groupBy, join and orderBy need all rows with the same key to be together, so Spark moves rows
between partitions. This movement is called a shuffle. It is expensive because rows are written to
disk, sent across the network and read again.

Data skew. During a shuffle, a row goes to partition hash(key) % number_of_partitions. If one key
has far more rows than the others, the partition that receives that key becomes far bigger than the
rest. This is data skew.

Why it is a bottleneck. A stage finishes only when its slowest task finishes. With skew, most tasks
finish quickly and one task does most of the work, so the whole job waits for that task.

How to identify it. Count the rows of each key (groupBy("key").count()), and check the Jobs and
Stages tabs of the Spark UI at http://localhost:4040 for stages with a shuffle and for tasks that
are much bigger or slower than the others.

How to fix it.

  Fix                              Idea
  -------------------------------- ----------------------------------------------------------------------------------------------------------------------
  Adaptive Query Execution (AQE)   Spark measures shuffle partitions at run time, merges small ones and splits skewed ones (spark.sql.adaptive.enabled)
  Tune shuffle partitions          Set spark.sql.shuffle.partitions to suit the data size (8 is used here instead of the default 200)
  Salting                          Add a random number to the skewed key so its rows spread over many partitions
  Broadcast join                   When joining with a small table, send the small table to every task so the big table is not shuffled

Files
-----

  File               Purpose
  ------------------ ------------------------------------------------------------------------------------
  skew.py            Spark job that creates skewed data, shows the key distribution and runs a GROUP BY
  requirements.txt   Python package needed (PySpark)

Requirements (Windows)
----------------------

-   Windows 10 or 11
-   Python 3.10 or above (tick Add python.exe to PATH during installation)
-   Java 17 (JDK), with JAVA_HOME set
-   PySpark (installed in step 3)

Steps to run (Windows PowerShell)
---------------------------------

1. Check Python and Java

    python --version
    java -version

If java is not found, install JDK 17 (for example Eclipse Temurin 17 from https://adoptium.net) and
choose Set JAVA_HOME variable in the installer. Close PowerShell and open it again after installing.

2. Download the code

    git clone https://github.com/show-star11/alf.git
    cd alf\practical-1

Without Git: on the GitHub page click Code > Download ZIP, extract it, then open PowerShell inside
the extracted folder.

3. Create a virtual environment and install PySpark

    python -m venv .venv
    .venv\Scripts\Activate.ps1
    pip install -r requirements.txt

If the second command gives a "running scripts is disabled" error, run this once and then repeat it:

    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

After activation the prompt starts with (.venv).

4. Run the Spark job

    python skew.py

5. Open the Spark UI

After GROUP BY completed! is printed, the program keeps Spark running for 120 seconds. In that time:

1.  Open http://localhost:4040 in a browser.
2.  Jobs tab: shows the 4 completed jobs.
3.  Click a job, then open its stages in the Stages tab: the Shuffle Write and Shuffle Read columns
    show where the shuffle happens.
4.  SQL / DataFrame tab: click a query to see the plan, including the Exchange step, which is the
    shuffle.

The program stops by itself after 120 seconds.

Code explanation
----------------

1.  Spark session. local[4] runs Spark on this computer with 4 cores.
    spark.sql.shuffle.partitions = 8 means every shuffle produces 8 partitions.
2.  Dataset. spark.range(5_000_000) creates 5 million rows with an id column.
3.  Skewed key. when(...).otherwise(...) gives key = 0 to the first 90% of the rows. The remaining
    10% get keys from 1 to 999.
4.  Key distribution. groupBy("key").count() sorted in descending order shows how many rows each key
    has. This is where the skew is identified.
5.  GROUP BY. groupBy("key").count() causes a shuffle. collect() is an action, so it forces Spark to
    run the job.
6.  Spark UI. time.sleep(120) keeps the application alive so the Spark UI can be opened before
    spark.stop().

Output
------

    ====================================
    Spark application started!
    ====================================
    Dataset created!
    Skewed dataset created!

    Key distribution:
    +---+-------+
    |key|  count|
    +---+-------+
    |  0|4500000|
    |517|    501|
    |518|    501|
    |519|    501|
    |537|    501|
    |564|    501|
    |566|    501|
    |569|    501|
    |577|    501|
    |582|    501|
    +---+-------+
    only showing top 10 rows

    Starting GROUP BY...

    GROUP BY completed!
    ====================================
    Open Spark UI:
    http://localhost:4040
    ====================================

On Windows, warnings such as Unable to load native-hadoop library, Did not find winutils.exe and
SUCCESS: The process with PID ... has been terminated may also appear. They are normal and can be
ignored.

Observations
------------

-   Key 0 has 4,500,000 rows out of 5,000,000 (90%), and the other keys have about 500 rows each.
    The data is heavily skewed.
-   The Spark UI shows 4 completed jobs for this program.
-   In the Stages tab, the first stage of the GROUP BY has 4 tasks and a Shuffle Write; the next
    stage has a Shuffle Read. This is the shuffle caused by groupBy.
-   Spark reduces the cost of this shuffle by itself: each task first counts the rows inside its own
    partition, so only about 1,000 partial results are shuffled instead of 5 million rows. AQE then
    merges the small shuffle partitions into one task.

Conclusion
----------

A Spark job was implemented on 5 million records with an intentionally skewed key. The skew was
identified from the key distribution, and the shuffle caused by GROUP BY was observed in the Spark
UI. Skew and shuffle bottlenecks are handled by Adaptive Query Execution, a suitable number of
shuffle partitions, salting of the skewed key and broadcast joins.
