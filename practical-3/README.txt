Big Data Practical 3: 3-Broker Kafka Cluster
============================================

Aim
---

Set up a 3-broker Apache Kafka cluster using Docker, and send and receive messages with a Python
producer and consumer.

Theory
------

Apache Kafka. Kafka is a distributed system for sending streams of messages between programs. One
program writes messages and other programs read them, without the two being connected directly.

  Term             Meaning
  ---------------- ------------------------------------------------------------------------------------------------
  Broker           A Kafka server that stores messages and serves producers and consumers
  Cluster          A group of brokers working together; this practical has 3
  Topic            A named stream of messages, here student-events
  Partition        A topic is split into partitions; each partition is an ordered log of messages
  Offset           The position number of a message inside a partition (0, 1, 2, ...)
  Producer         A program that sends messages to a topic
  Consumer         A program that reads messages from a topic
  Consumer group   Consumers with the same group_id share the work and Kafka remembers how far the group has read

Why 3 brokers. With more than one broker, the cluster keeps working when one broker stops, and
partitions can be copied (replicated) to other brokers so messages are not lost.

KRaft mode. Older Kafka versions needed a separate ZooKeeper service to manage the cluster. In KRaft
mode, the brokers manage the cluster themselves. Here each of the 3 nodes is both a broker and a
controller, and the 3 controllers vote to elect a leader.

Docker. Each broker runs in its own container, started together from one docker-compose.yml file, so
Kafka does not have to be installed on Windows.

Files
-----

  File                 Purpose
  -------------------- -------------------------------------------------------
  docker-compose.yml   Defines the 3 Kafka brokers (kafka1, kafka2, kafka3)
  producer.py          Sends 10 student messages to the topic student-events
  consumer.py          Reads messages from student-events and prints them
  requirements.txt     Python package needed (kafka-python)

Requirements (Windows)
----------------------

-   Windows 10 or 11
-   Docker Desktop, running
-   Python 3.10 or above
-   VS Code

Steps to run (Windows PowerShell)
---------------------------------

Step 1. Check Docker

Install Docker Desktop from https://www.docker.com/products/docker-desktop/ if it is not installed,
then open it and wait until it shows Engine running. In the VS Code terminal:

    docker --version
    docker ps

docker ps should print the column headings without an error.

Step 2. Create the project in VS Code

    git clone https://github.com/show-star11/alf.git
    cd alf\practical-3

Open this folder in VS Code. It contains docker-compose.yml, producer.py and consumer.py.

Step 3. Start Kafka using Docker

    docker compose up -d
    docker ps

The first run downloads the apache/kafka:3.7.0 image. docker ps should list three containers,
kafka1, kafka2 and kafka3, with status Up. Wait about 20 seconds before the next step so the brokers
finish starting.

Step 4. Install the Python Kafka library

    pip install kafka-python

Step 5. Send and receive messages

Open two terminals in VS Code (click the split icon in the terminal panel).

Terminal 1, start the consumer first:

    python consumer.py

Terminal 2, run the producer:

    python producer.py

The producer prints 10 Produced: lines, one per second, and the consumer prints the matching
Received: lines. Stop the consumer with Ctrl + C.

Step 6. Stop Kafka

    docker compose down

Add -v (docker compose down -v) to also delete the stored messages.

Code explanation
----------------

docker-compose.yml

  Setting                                   Meaning
  ----------------------------------------- ----------------------------------------------------------------------------------------------------------------------------------------------------------
  image: apache/kafka:3.7.0                 The official Kafka image, version 3.7.0
  ports: "29092:29092"                      Opens the broker to programs on Windows. The three brokers use ports 29092, 39092 and 49092
  KAFKA_NODE_ID                             A different ID for every broker: 1, 2, 3
  KAFKA_PROCESS_ROLES: broker,controller    Each node stores data and also helps manage the cluster (KRaft mode)
  KAFKA_LISTENERS                           The three connections a broker accepts: PLAINTEXT (9092) between brokers, CONTROLLER (9093) for cluster management, EXTERNAL (29092) for Python programs
  KAFKA_ADVERTISED_LISTENERS                The address a broker tells clients to use: kafka1:9092 inside Docker, localhost:29092 from Windows
  KAFKA_CONTROLLER_QUORUM_VOTERS            The list of all 3 controllers, so they can find each other and elect a leader
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"   The topic student-events is created automatically when the producer first sends to it
  volumes                                   Keeps each broker's data when the containers are restarted

producer.py

1.  KafkaProducer(bootstrap_servers=[...]) connects to the cluster using the three broker addresses.
2.  value_serializer converts each Python dictionary to JSON and then to bytes, because Kafka stores
    bytes.
3.  The loop builds 10 student messages and sends each one to student-events with producer.send(),
    waiting 1 second between messages.
4.  producer.flush() makes sure every message has been sent before producer.close().

consumer.py

1.  KafkaConsumer("student-events", ...) subscribes to the topic on the same three brokers.
2.  auto_offset_reset="earliest" starts from the first message when this group has not read the
    topic before.
3.  group_id="student-consumer-group" puts the consumer in a group, so Kafka remembers its position.
4.  value_deserializer converts the bytes back to a Python dictionary.
5.  The for loop waits for messages and prints each one with its partition and offset.

Output
------

docker ps after step 3:

    IMAGE                COMMAND                  STATUS          PORTS                      NAMES
    apache/kafka:3.7.0   "/__cacert_entrypoin…"   Up 17 seconds   0.0.0.0:39092->29092/tcp   kafka2
    apache/kafka:3.7.0   "/__cacert_entrypoin…"   Up 17 seconds   0.0.0.0:49092->29092/tcp   kafka3
    apache/kafka:3.7.0   "/__cacert_entrypoin…"   Up 17 seconds   0.0.0.0:29092->29092/tcp   kafka1

Producer:

    Produced: {'student_id': 1, 'name': 'Student-1', 'course': 'Data Science', 'message': 'Hello from student 1'}
    Produced: {'student_id': 2, 'name': 'Student-2', 'course': 'Data Science', 'message': 'Hello from student 2'}
    ...
    Produced: {'student_id': 10, 'name': 'Student-10', 'course': 'Data Science', 'message': 'Hello from student 10'}

Consumer:

    Consumer started...
    Waiting for messages...

    Received: {'student_id': 1, 'name': 'Student-1', 'course': 'Data Science', 'message': 'Hello from student 1'} | Partition: 0 | Offset: 0
    Received: {'student_id': 2, 'name': 'Student-2', 'course': 'Data Science', 'message': 'Hello from student 2'} | Partition: 0 | Offset: 1
    ...
    Received: {'student_id': 10, 'name': 'Student-10', 'course': 'Data Science', 'message': 'Hello from student 10'} | Partition: 0 | Offset: 9

A DeprecationWarning: value_deserializer does not implement kafka.serializer.Deserializer line may
appear when the consumer starts. It is only a warning and the program works.

Observations
------------

-   Three Kafka brokers run as three Docker containers and form one cluster.
-   All 10 messages sent by the producer are received by the consumer in the same order.
-   Every message is in partition 0 and the offsets go from 0 to 9, because the automatically
    created topic has one partition.
-   Running the consumer again prints nothing new, because the consumer group has already read up to
    offset 9.

Errors on Windows and how to fix them
-------------------------------------
Error 1: docker : The term 'docker' is not recognized

Cause: Docker Desktop is not installed, or it is installed and running but the terminal cannot find
the docker program.

If Docker Desktop is not installed:

1.  Download it from https://www.docker.com/products/docker-desktop/ and run the installer.
2.  Keep Use WSL 2 instead of Hyper-V ticked and restart the laptop when it asks.
3.  Open Docker Desktop and wait until the bottom-left corner shows Engine running.
4.  Close VS Code completely, open it again and run docker --version.

If Docker Desktop is running but the terminal still does not find docker:

1.  Run these lines one at a time in the same terminal. They find where Docker is installed and add
    it to the terminal's path. The first two lines can take up to a minute.
        $d = Split-Path (Get-Process "Docker Desktop" | Select-Object -First 1).Path
        $exe = Get-ChildItem $d, (Split-Path $d), "C:\Program Files\Docker", "$env:LOCALAPPDATA\Programs" -Filter docker.exe -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
        $exe.FullName
        $env:PATH += ";" + $exe.DirectoryName
        docker --version
2.  The third line prints the location of docker.exe, and the last line prints the Docker version.
3.  Continue with docker compose up -d in the same terminal. This fix lasts until the terminal is
    closed; restarting the laptop usually makes it permanent.

If that does not work, use Docker's own terminal:

1.  Open the Docker Desktop window and click the >_ icon at the bottom right corner.
2.  In the terminal that opens, go to the project folder and run the Docker commands there:
        cd "C:\path\to\alf\practical-3"
        docker compose up -d
        docker ps

Error 2: The term 'C:\Users\...' is not recognized when opening the project folder

Cause: the folder path was typed without cd, or the path contains spaces and was not put in quotes.

1.  Type cd first and put the whole path in double quotes:
        cd "C:\Users\HP\OneDrive\Desktop\big data1\practical 3"
2.  git clone puts the files in a folder named alf, and this practical is in its practical-3
    subfolder, so go into it:
        cd alf\practical-3
        dir
3.  dir should list docker-compose.yml, producer.py and consumer.py.

Error 3: error during connect on docker ps

Cause: Docker Desktop is not running.

1.  Open Docker Desktop from the Start menu.
2.  Wait until the bottom-left corner shows Engine running.
3.  Run docker ps again.

Error 4: NoBrokersAvailable when running the Python programs

Cause: the brokers are not running or have not finished starting.

1.  Run docker ps and check that kafka1, kafka2 and kafka3 all show Up.
2.  If they are missing, run docker compose up -d in the project folder.
3.  Wait 20 to 30 seconds and run the program again.

Error 5: port is already allocated

Cause: another program or an old container is using port 29092, 39092 or 49092.

1.  Run docker compose down.
2.  Run docker ps -a and remove any old Kafka container with docker rm -f <container-name>.
3.  Run docker compose up -d again.

Error 6: ModuleNotFoundError: No module named 'kafka'

Cause: the Kafka library is not installed in the Python being used.

1.  Run pip install kafka-python.
2.  Run the program again with the same python command.

Error 7: the consumer prints nothing

Cause: this consumer group has already read all the messages.

1.  Keep the consumer running.
2.  Run python producer.py in the second terminal to send new messages.

Conclusion
----------

A 3-broker Kafka cluster was set up in KRaft mode using Docker Compose. A Python producer sent 10
JSON messages to the topic student-events, and a Python consumer received all of them with their
partition and offset, which shows the cluster working end to end.
