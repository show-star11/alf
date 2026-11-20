Big Data Practical 5: Real-Time Word Count with Spark Structured Streaming
==========================================================================

Aim
---

Implement real-time stream processing with Spark Structured Streaming: read text sent through a
socket and display a running word count.

Theory
------

Batch and stream processing. Batch processing works on data that is already stored, such as a file.
Stream processing works on data that keeps arriving, such as sensor readings, logs or chat messages,
and updates the result as new data comes in.

Spark Structured Streaming. Structured Streaming is the stream processing engine of Spark. A stream
is treated as a table that keeps growing: every new line becomes a new row. The same DataFrame
operations used on normal data (select, groupBy, count) work on this table, and Spark updates the
result after every small batch of new data. Each update is called a micro-batch and is printed as
Batch: 0, Batch: 1 and so on.

  Part                             In this practical
  -------------------------------- -----------------------------------------------------
  Source (where data comes from)   A socket on localhost:9999, fed by socket_sender.py
  Processing                       Split each line into words and count each word
  Sink (where results go)          The console (terminal)

Socket. A socket is a connection between two programs over the network. socket_sender.py opens port
9999 and waits; Spark connects to it and receives every line typed into the sender.

Output modes.

  Mode       What is written after each batch
  ---------- -------------------------------------------------------------------------------
  complete   The whole result table, with the total count of every word so far (used here)
  update     Only the rows that changed in this batch
  append     Only new rows; not allowed for this kind of aggregation

Checkpoint. Spark saves the progress of the stream (which data has been read and the counts so far)
in the checkpoint folder. If the query is restarted, it can continue from there instead of starting
again.

Files
-----

  File                   Purpose
  ---------------------- -------------------------------------------------------------------------------------
  socket_sender.py       Opens a socket on port 9999, waits for Spark, then sends every line you type
  word_count_stream.py   Spark Structured Streaming program that reads the socket and prints the word counts
  requirements.txt       Python package needed (PySpark 4.2.0)

Requirements (Windows)
----------------------

1.  Python 3.10 or above
2.  Java 17 or 21 (JDK)
3.  PySpark
4.  Spark (included in the PySpark package)
5.  VS Code
6.  winutils.exe and hadoop.dll in C:\hadoop\bin (see Step 3)

Java can be downloaded from https://adoptium.net (Temurin 17 or 21) or
https://www.oracle.com/in/java/technologies/downloads/.

Steps to run (Windows PowerShell)
---------------------------------

Run every command in the VS Code terminal. If a step does not give the expected result, the error
number in brackets points to the fix in Errors on Windows and how to fix them.

Step 1. Get the project and open it in VS Code

    git clone https://github.com/show-star11/alf.git
    cd alf\practical-5

If the folder path has spaces, put it in double quotes, for example
cd "C:\Users\HP\OneDrive\Desktop\big data1\practical 5". Open the folder in VS Code with File > Open
Folder.

Step 2. Check Python, Java and PySpark

    python --version
    java -version
    pip install pyspark
    python -c "import pyspark; print(pyspark.__version__)"

Expected: a Python version, a Java version (17 or 21), and then the PySpark version, for example
4.2.0. (Errors 1, 2 and 3.)

To see where PySpark is installed:

    python -m pip show pyspark

The Location: line shows the folder. The command pyspark --version may say
'pyspark' is not recognized; this is normal and does not stop the practical, because the programs
are run with python. (Error 1.)

Step 3. Set up Hadoop for Windows (needed once)

Spark on Windows needs winutils.exe and hadoop.dll to write the checkpoint folder.

1.  Create the folder:
        mkdir C:\hadoop\bin
2.  Open https://github.com/cdarlint/winutils, open the newest hadoop-3.3.x folder, then bin.
3.  Download winutils.exe and hadoop.dll and move both into C:\hadoop\bin.
4.  Check the exact file names:
        Get-ChildItem C:\hadoop\bin | Select-Object Name, Length

    The names must be exactly hadoop.dll and winutils.exe. A browser can save a download as
    hadoop (1).dll or similar; rename it if so (Error 5).
5.  Copy hadoop.dll into the Windows system folder. Open Windows PowerShell with Run as
    administrator and run:
        Copy-Item C:\hadoop\bin\hadoop.dll C:\Windows\System32\

    Then close VS Code completely and open it again.

If this was already done for Practical 2, skip this step.

Step 4. Check the checkpoint drive

word_count_stream.py saves its checkpoint in D:\spark-checkpoints\wordcount. Check that the laptop
has a D: drive:

    Test-Path D:\

If this prints False, open word_count_stream.py and change both lines that say
file:///D:/spark-checkpoints/wordcount to file:///C:/spark-checkpoints/wordcount, then save. (Error
6.)

Step 5. Terminal 1: start the socket sender

    python socket_sender.py

Expected:

    ======================================
    Socket Server Started
    Waiting for Spark to connect...
    Host: localhost
    Port: 9999
    ======================================

Keep this terminal open. (Error 7 if port 9999 is in use.)

Step 6. Terminal 2: start the Spark streaming program

Open a second terminal (click + in the terminal panel). Set the Hadoop variables for this terminal,
then run the program:

    $env:HADOOP_HOME = "C:\hadoop"
    $env:PATH = "C:\hadoop\bin;$env:PATH"
    $env:_JAVA_OPTIONS = "-Dhadoop.home.dir=C:/hadoop"
    $env:PYSPARK_PYTHON = "python"
    python word_count_stream.py

Wait for Spark to start (10 to 30 seconds). Terminal 1 now shows Spark connected! and
Enter message:, and Terminal 2 shows an empty Batch: 0. (Errors 4, 5 and 8.)

The PYSPARK_PYTHON line removes the warning Missing Python executable 'python3'. The start-up lines
Picked up _JAVA_OPTIONS, WARNING: Using incubator modules and Using Spark's default log4j profile
are normal.

Step 7. Type messages in Terminal 1

Go back to Terminal 1 and type one line at a time, pressing Enter after each:

    hello spark
    hello world
    hello raj

Step 8. See the word counts in Terminal 2

After each message, Terminal 2 prints a new batch with the updated count of every word (see Output
below).

Step 9. Stop the programs

1.  In Terminal 1, type exit and press Enter.
2.  In Terminal 2, press Ctrl + C to stop Spark.

Code explanation
----------------

socket_sender.py

1.  socket.socket(socket.AF_INET, socket.SOCK_STREAM) creates a TCP socket.
2.  bind((HOST, PORT)) attaches it to localhost port 9999, and listen(1) waits for one connection.
3.  accept() waits until Spark connects and returns the connection.
4.  The while loop reads a line with input() and sends it to Spark with sendall(), adding \n so
    Spark sees the end of the line.
5.  Typing exit breaks the loop and closes the connection and the socket.

word_count_stream.py

1.  SparkSession.builder.master("local[2]") starts Spark on this computer with 2 threads. A socket
    stream needs at least 2: one to receive data and one to process it.
2.  spark.sql.streaming.checkpointLocation sets the folder where Spark saves the stream's progress.
3.  setLogLevel("ERROR") hides warning messages so the counts are easy to read.
4.  spark.readStream.format("socket") with host and port reads each line from the socket into a
    column named value.
5.  split(data.value, " ") cuts each line into words, and explode(...) turns the list of words into
    one row per word, named word.
6.  groupBy("word").count() counts how many times each word has appeared.
7.  writeStream.outputMode("complete").format("console") prints the full result table to the
    terminal after every batch.
8.  start() begins the stream, and awaitTermination() keeps the program running until it is stopped.

Output
------

Terminal 1 (socket_sender.py):

    ======================================
    Socket Server Started
    Waiting for Spark to connect...
    Host: localhost
    Port: 9999
    ======================================
    Spark connected!
    Start entering messages.
    Type 'exit' to stop.
    --------------------------------------
    Enter message: hello spark
    Enter message: hello world
    Enter message: hello raj
    Enter message:

Terminal 2 (word_count_stream.py):

    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +----+-----+
    |word|count|
    +----+-----+
    +----+-----+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +-----+-----+
    | word|count|
    +-----+-----+
    |hello|    2|
    |spark|    1|
    |world|    1|
    +-----+-----+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +-----+-----+
    | word|count|
    +-----+-----+
    |hello|    3|
    |  raj|    1|
    |spark|    1|
    |world|    1|
    +-----+-----+

How many messages land in each batch depends on how fast they are typed: two messages typed close
together can appear in the same batch, as in Batch 1 above. The final counts are always the same.

Observations
------------

-   Batch 0 is empty because Spark creates the first batch before any message arrives.
-   After each new message, Spark prints a new batch with the updated total for every word, because
    the output mode is complete.
-   The count of hello grows from 2 to 3 across batches, which shows that Spark keeps the state of
    the counts between batches.
-   Words are counted exactly as typed, so Hello and hello are counted as different words.

Errors on Windows and how to fix them
-------------------------------------
Error 1: 'pyspark' is not recognized as an internal or external command

Also appears as Python was not found; run without arguments to install from the Microsoft Store when
running pyspark --version.

Cause: PySpark is installed as a Python package, but the pyspark shell command is not on the Windows
PATH. This practical does not need that command.

1.  Check PySpark through Python instead:
        python -c "import pyspark; print(pyspark.__version__)"
2.  To see where it is installed:
        python -m pip show pyspark
3.  If the version is printed, PySpark is installed correctly and you can continue.

Error 2: ModuleNotFoundError: No module named 'pyspark'

Cause: PySpark is installed for a different Python than the one running the program.

1.  Install it with the same Python that runs the program:
        python -m pip install pyspark
2.  In VS Code, press Ctrl + Shift + P, choose Python: Select Interpreter and select the same
    Python.

Error 3: 'java' is not recognized, JAVA_HOME is not set or Java gateway process exited

Cause: Java is not installed, or it is a version PySpark 4.2 does not support.

1.  Run java -version. PySpark 4.2 needs Java 17 or 21.
2.  If Java is missing or a different version, install JDK 17 or 21 from https://adoptium.net and
    choose Set JAVA_HOME variable in the installer.
3.  Close VS Code completely and open it again.

Error 4: HADOOP_HOME and hadoop.home.dir are unset or Did not find winutils.exe

Cause: Spark on Windows needs winutils.exe to create the checkpoint folder.

1.  Do Step 3 to put winutils.exe and hadoop.dll in C:\hadoop\bin.
2.  Run the three $env: lines from Step 6 in the same terminal before python word_count_stream.py.
3.  To set them permanently, run this once and then restart VS Code:
        setx HADOOP_HOME "C:\hadoop"
        setx PATH "$env:PATH;C:\hadoop\bin"

Error 5: UnsatisfiedLinkError: ... NativeIO$Windows.access0

Cause: Spark cannot load hadoop.dll. Either the file is missing, it was saved under a different name
(for example hadoop (1).dll), or Spark does not find it in C:\hadoop\bin.

1.  List the exact file names:
        Get-ChildItem C:\hadoop\bin | Select-Object Name, Length
2.  If a name is not exactly hadoop.dll or winutils.exe, rename it, for example:
        Rename-Item "C:\hadoop\bin\hadoop (1).dll" "hadoop.dll"

    If hadoop.dll is missing, download it from the same hadoop-3.3.x/bin folder of
    https://github.com/cdarlint/winutils as winutils.exe.
3.  Open Windows PowerShell with Run as administrator and copy the file into the Windows system
    folder:
        Copy-Item C:\hadoop\bin\hadoop.dll C:\Windows\System32\
        dir C:\Windows\System32\hadoop.dll
4.  Close VS Code completely, open it again and repeat Steps 5 and 6.

Error 6: The system cannot find the path specified or an error about D:/spark-checkpoints

Cause: the laptop has no D: drive, so Spark cannot create the checkpoint folder.

1.  Open word_count_stream.py.
2.  Change both lines with file:///D:/spark-checkpoints/wordcount to
    file:///C:/spark-checkpoints/wordcount.
3.  Save and run the program again.

Error 7: OSError: [WinError 10048] Only one usage of each socket address ... is normally permitted

Cause: port 9999 is still used by an earlier socket_sender.py that did not close.

1.  Close the old terminal that is running socket_sender.py, or press Ctrl + C in it.
2.  If it still fails, find and stop the program using the port:
        netstat -ano | findstr :9999
        taskkill /PID <number-in-last-column> /F
3.  Run python socket_sender.py again.

Error 8: Connection refused when starting word_count_stream.py

Cause: Spark was started before the socket sender, so there was nothing to connect to.

1.  Stop Spark with Ctrl + C.
2.  Start python socket_sender.py in Terminal 1 first and wait for Waiting for Spark to connect....
3.  Then start python word_count_stream.py in Terminal 2.

Error 9: the counts start from old values or the query fails after a restart

Cause: the checkpoint folder still holds the state of an earlier run.

1.  Stop both programs.
2.  Delete the checkpoint folder:
        Remove-Item -Recurse -Force D:\spark-checkpoints\wordcount

    Use C:\spark-checkpoints\wordcount if the path was changed in Error 6.
3.  Start again from Step 5.

Error 10: nothing appears in Terminal 2 after typing a message

Cause: the message was typed before Spark connected, or Spark is still starting.

1.  Check that Terminal 1 shows Spark connected!.
2.  Wait until Terminal 2 shows Batch: 0, then type the message again.

Conclusion
----------

A socket server was used as a live data source and Spark Structured Streaming read each line from it
as it arrived. The lines were split into words and counted, and the running counts were printed to
the console after every micro-batch, which shows real-time stream processing with Spark.
