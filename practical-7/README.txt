Big Data Practical 7: Watermark Handling with Late Data and Side Outputs
========================================================================

Aim
---

Watermark Handling: Create a stream processor that correctly handles "Late Data" using watermarks
and side outputs.

Theory
------

Stream processing. Stream processing handles data as soon as it arrives instead of waiting for all
of it. A food delivery app, for example, keeps receiving updates such as order placed, order
confirmed, food picked up and order delivered, and updates the order status each time. Other
examples are traffic monitoring, fraud detection, sensor monitoring and online payments.

Event time and processing time. Event time is when something actually happened. Processing time is
when the system received it. If a rider marks an order as picked up at 10:00:00 and the server
receives it at 10:00:05, the event time is 10:00:00 and the processing time is 10:00:05. Event time
is used when the order of events matters.

Late data. Late data is an event that reaches the system after events that happened later than it.
It is caused by poor internet, network delays, devices that were offline, or systems running at
different speeds.

  Event time   Arrival order   Event
  ------------ --------------- --------------------------
  10:00:01     1               Order picked up
  10:00:08     2               Rider reached restaurant
  10:00:03     3               Order confirmed

Here the 10:00:03 event arrives after the 10:00:08 event.

Watermark. A watermark is Flink's estimate of how far event time has progressed. A watermark of 10
seconds means "events before 10 seconds should have arrived by now". It tells Flink when a time
window can be closed. It is only an estimate: an older event can still arrive afterwards, and that
event is late.

Bounded out-of-orderness. for_bounded_out_of_orderness(2 seconds) lets events arrive up to 2 seconds
out of order. The watermark is the largest event time seen so far minus 2 seconds.

Tumbling window. A tumbling window groups events into fixed time slots that do not overlap, for
example 0-10 s, 10-20 s and 20-30 s. A window gives its result when the watermark passes its end
time.

Side output. A side output is a second output stream for special records. Here, events that arrive
after their window has already closed are sent to the side output instead of being lost. They can
then be stored, checked for network problems, or processed separately.

How they work together.

  Step   What happens
  ------ ------------------------------------------------------------------------------
  1      Events at 1 s and 8 s arrive and go into window 0-10 s
  2      An event at 15 s arrives; the watermark becomes 13 s
  3      The watermark has passed 10 s, so window 0-10 s closes and prints its result
  4      The event at 3 s arrives; its window is already closed, so it is late
  5      The late event goes to the side output

What was changed from the practical sheet, and why
--------------------------------------------------

The code in the practical sheet does not run, and even after a fix it never produces a late event.
These changes were made:

1.  Timestamp assigner. with_timestamp_assigner(lambda event, timestamp: event[1]) stops with
    AttributeError: 'function' object has no attribute 'extract_timestamp', because PyFlink needs a
    TimestampAssigner class, not a lambda. Change: added the class DeliveryTimestampAssigner.
2.  No late event in the data. In the sheet's data, the highest time before the 3 s event is 8 s, so
    the watermark is only 6 s and window 0-10 s is still open; the 3 s event is not late. Change:
    added the event Rider on the way at 15 s before it, so the watermark passes 10 s first.
3.  Events arrive too fast. All events are read in a few milliseconds, so Flink has no time to
    create a watermark between them, and nothing is late. Change: each event is delayed by 1 second
    with slow_down, PyFlink passes each event on at once (bundle size 1), and the operators run
    separately.
4.  Typed side output tag. OutputTag("late_events", Types.TUPLE(...)) fails with
    cannot pickle '_thread.RLock' object in PyFlink 1.20. Change: the tag is created without a type.
5.  Window result. reduce(lambda a, b: a) keeps only the first event of each window. Change: the
    reduce joins the names of all events in the window.
6.  Labels. Both outputs printed the same way. Change: they are labelled ON TIME WINDOW and
    LATE EVENT.

Files
-----

  File               Purpose
  ------------------ ----------------------------------------
  latedata.py        The PyFlink program
  requirements.txt   Python package needed (PyFlink 1.20.3)

Requirements (Windows)
----------------------

1.  Python 3.11. PyFlink 1.20 does not install on Python 3.12 or 3.13.
2.  A project folder with no spaces in its path, for example C:\flink\practical7. PyFlink on Windows
    cannot start its Python workers from a path with spaces.
3.  Java JDK 17
4.  PyFlink (apache-flink 1.20.3)
5.  VS Code

Step 1. Install Java 17
-----------------------

Download Temurin 17 from https://adoptium.net and install it, with Set JAVA_HOME variable and Add to
PATH turned on. Check in a new terminal:

    java -version

It should start with openjdk version "17. If Java 17 is already installed for Practical 6, skip this
step.

Step 2. Install Python 3.11
---------------------------

PyFlink needs Python 3.11 even if a newer Python is already installed. Both can be installed side by
side.

1.  Open https://www.python.org/downloads/windows/ and download the Windows installer (64-bit) of
    the latest Python 3.11.x.
2.  Run it and tick Add python.exe to PATH, then click Install Now.
3.  Check in a new terminal:
        py -3.11 --version

    It should print Python 3.11.x.

Step 3. Get the project into a folder without spaces
----------------------------------------------------

PyFlink on Windows fails with Process died with exit code 0 when the project path has a space, such
as C:\Users\HP\big data\practical 7. Use a short path without spaces:

    mkdir C:\flink
    cd C:\flink
    git clone https://github.com/show-star11/alf.git
    cd alf\practical-7

Or create the folder C:\flink\practical7, open it in VS Code with File > Open Folder, create a file
named latedata.py, and paste the code from Step 6.

Step 4. Create a Python 3.11 environment
----------------------------------------

    py -3.11 -m venv .venv
    .venv\Scripts\Activate.ps1
    python --version

The prompt now starts with (.venv) and python --version prints Python 3.11.x. If activation gives a
"running scripts is disabled" error, run Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
and repeat the second line.

Step 5. Install PyFlink
-----------------------

    pip install -r requirements.txt

Or without the file: pip install apache-flink==1.20.3. This downloads about 300 MB and takes a few
minutes. Check:

    python -c "import pyflink; print('PyFlink OK')"

Step 6. The code: latedata.py
-----------------------------

    import sys
    import time

    from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
    from pyflink.common.time import Time
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.output_tag import OutputTag
    from pyflink.datastream.window import TumblingEventTimeWindows

    # Tells Flink where the event time is stored in each event
    class DeliveryTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return value[1]

    def slow_down(event):
        # Send one event every second, like events arriving over a network
        time.sleep(1)
        return event

    # Send each event on as soon as it is processed, instead of in batches
    config = Configuration()
    config.set_string("python.fn-execution.bundle.size", "1")
    config.set_string("python.fn-execution.bundle.time", "10")
    config.set_string("python.operator-chaining.enabled", "false")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.disable_operator_chaining()

    # Run the Python parts of the job with the same Python that runs this script
    env.set_python_executable(sys.executable)

    # Create a watermark every 100 ms
    env.get_config().set_auto_watermark_interval(100)

    # Delivery events: order status and event timestamp (milliseconds),
    # in the order they ARRIVE at Flink
    events = [
        ("Picked up", 1000),
        ("At restaurant", 8000),
        ("Rider on the way", 15000),
        ("Order confirmed", 3000),      # happened at 3 s but arrives after 15 s: LATE
        ("Delivered", 22000)
    ]

    event_type = Types.TUPLE([Types.STRING(), Types.LONG()])

    data = env.from_collection(events, type_info=event_type) \
        .map(slow_down, output_type=event_type)

    # Allow events to arrive up to 2 seconds out of order
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(2)
    ).with_timestamp_assigner(DeliveryTimestampAssigner())

    stream = data.assign_timestamps_and_watermarks(watermarks)

    # Side output for late events
    # (no type is given here: PyFlink 1.20 cannot send a typed OutputTag to the
    #  window operator and fails with "cannot pickle '_thread.RLock' object")
    late_tag = OutputTag("late_events")

    # Create 10-second windows
    windowed = (
        stream
        .key_by(lambda event: "delivery", key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .side_output_late_data(late_tag)
    )

    # Main output: join the names of all events in each window
    main = windowed.reduce(
        lambda a, b: (a[0] + ", " + b[0], max(a[1], b[1])),
        output_type=event_type
    )

    # Late events that arrived after their window closed
    late = main.get_side_output(late_tag)

    main.print("ON TIME WINDOW")
    late.print("LATE EVENT")

    env.execute("Food Delivery Late Data")

Step 7. Run the program
-----------------------

With (.venv) showing in the prompt:

    python latedata.py

It takes 20 to 40 seconds: Flink starts, then the events arrive one per second.

Output
------

    ON TIME WINDOW> (Picked up, At restaurant,8000)
    LATE EVENT> ('Order confirmed', 3000)
    ON TIME WINDOW> (Rider on the way,15000)
    ON TIME WINDOW> (Delivered,22000)

-   ON TIME WINDOW> (Picked up, At restaurant,8000): window 0-10 s, with the events that arrived in
    time; 8000 is the latest event time in it.
-   LATE EVENT> ('Order confirmed', 3000): the 3 s event arrived after window 0-10 s had closed, so
    it went to the side output.
-   ON TIME WINDOW> (Rider on the way,15000): window 10-20 s.
-   ON TIME WINDOW> (Delivered,22000): window 20-30 s, closed when the input ends.

Warnings printed when Flink starts can be ignored.

Code explanation
----------------

1.  Imports. WatermarkStrategy, TimestampAssigner, OutputTag and TumblingEventTimeWindows are the
    parts of PyFlink used for event time, late data and windows.
2.  DeliveryTimestampAssigner. extract_timestamp returns value[1], the event time in milliseconds,
    so Flink uses event time instead of arrival time.
3.  slow_down. Waits 1 second before passing each event on, so the events arrive spread over time
    like real network messages and Flink can create watermarks between them.
4.  Configuration. Bundle size 1 makes PyFlink pass every event on immediately. Turning off operator
    chaining runs each step separately, so the waiting in slow_down does not hold back the
    watermarks.
5.  set_parallelism(1). Uses one task, so the output order is easy to follow.
6.  set_auto_watermark_interval(100). Flink creates a new watermark every 100 ms.
7.  events. Five delivery events, listed in the order they arrive. Order confirmed happened at 3 s
    but arrives after the 15 s event.
8.  from_collection. Turns the list into a stream of (status, time) tuples.
9.  for_bounded_out_of_orderness(Duration.of_seconds(2)). Allows 2 seconds of disorder: watermark =
    largest event time seen - 2 s.
10. assign_timestamps_and_watermarks. Attaches the event time and the watermarks to the stream.
11. OutputTag("late_events"). The name of the side output for late events.
12. key_by(lambda event: "delivery"). Puts all events under one key, as they all belong to one
    delivery.
13. TumblingEventTimeWindows.of(Time.seconds(10)). Groups events into 10-second windows by event
    time.
14. side_output_late_data(late_tag). Sends events that arrive after their window closed to the side
    output.
15. reduce. Combines the events of each window into one result: the names joined with commas and the
    latest event time.
16. get_side_output(late_tag). Gets the stream of late events.
17. print(...) and execute(...). Print both streams with a label and start the job.

Observations
------------

-   Picked up (1 s) and At restaurant (8 s) arrive in time and are grouped in window 0-10 s.
-   When Rider on the way (15 s) arrives, the watermark becomes 13 s. This is past 10 s, so window
    0-10 s closes and its result is printed.
-   Order confirmed (3 s) arrives after that, so it is late and goes to the side output instead of
    being lost.
-   Without the side output, the late event would be dropped silently.

Errors on Windows and how to fix them
-------------------------------------
Error 1: pip install apache-flink fails, or No matching distribution found

Cause: the environment uses Python 3.12 or 3.13. PyFlink 1.20 supports Python 3.8 to 3.11 only.

1.  Check with python --version inside the environment.
2.  If it is not 3.11, delete the environment and create it again with Python 3.11:
        deactivate
        Remove-Item -Recurse -Force .venv
        py -3.11 -m venv .venv
        .venv\Scripts\Activate.ps1
        pip install -r requirements.txt

Error 2: Microsoft Visual C++ 14.0 or greater is required

Cause: a package is being built from source code during installation.

1.  Run python -m pip install --upgrade pip setuptools wheel.
2.  Run pip install -r requirements.txt again.
3.  If it still fails, install Microsoft C++ Build Tools from
    https://visualstudio.microsoft.com/visual-cpp-build-tools/ (select Desktop development with
    C++), restart the computer and try again.

Error 3: 'py' is not recognized

Cause: the Python launcher was not installed.

1.  Use the full path to Python 3.11 instead, for example:
        & "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m venv .venv

Error 4: JAVA_HOME is not set or Java gateway process exited

Cause: Java is missing or not found.

1.  Run java -version. It should be Java 17.
2.  If not, install JDK 17 (Step 1), close VS Code completely and open it again.

Error 5: AttributeError: 'function' object has no attribute 'extract_timestamp'

Cause: the code from the practical sheet passes a lambda to with_timestamp_assigner.

1.  Use the DeliveryTimestampAssigner class from Step 6 and pass DeliveryTimestampAssigner().

Error 6: TypeError: cannot pickle '_thread.RLock' object

Cause: the side output tag was created with a type, OutputTag("late_events", Types.TUPLE(...)).

1.  Create it without a type: late_tag = OutputTag("late_events").

Error 7: no LATE EVENT line in the output

Cause: the events reached Flink too quickly, so no watermark was created before the late event, and
it was put into its window as normal.

1.  Check that the code has the slow_down map, the three config.set_string lines and
    env.disable_operator_chaining().
2.  On a slow computer, increase the wait in slow_down to time.sleep(2).

Error 8: ModuleNotFoundError: No module named 'pyflink' while the job runs

Cause: Flink started its Python workers with a different Python.

1.  Activate the environment (.venv\Scripts\Activate.ps1) before running.
2.  Check that the code contains env.set_python_executable(sys.executable).

Error 9: Failed to create stage bundle factory and Process died with exit code 0

Cause: Flink could not start its Python worker. On Windows this happens when the project or its
.venv is in a folder whose path has a space, for example C:\Users\HP\big data\practical 7.

1.  Leave the environment and copy the code to a folder without spaces:
        deactivate
        mkdir C:\flink\practical7
        copy latedata.py C:\flink\practical7\
        cd C:\flink\practical7
2.  Create a new environment there; an existing .venv cannot be moved:
        py -3.11 -m venv .venv
        .venv\Scripts\Activate.ps1
        pip install apache-flink==1.20.3
3.  Run python latedata.py again.
4.  If it still fails, show the end of Flink's worker log and look for the error there:
        Get-ChildItem .venv\Lib\site-packages\pyflink\log | Sort-Object LastWriteTime | Select-Object -Last 1 | Get-Content -Tail 20

Seeing the real cause of an error

The first lines of a Flink error (Job execution failed) do not say what went wrong. To show only the
cause lines, run:

    python latedata.py 2>&1 | Select-String "Caused by" -Context 0,3

Conclusion
----------

A PyFlink stream processor was built for food delivery events. Event time was taken from each event,
watermarks with 2 seconds of allowed disorder tracked the progress of event time, and 10-second
tumbling windows grouped the events. The event that arrived after its window had closed was not
lost: it was sent to a side output and printed separately as a late event.
