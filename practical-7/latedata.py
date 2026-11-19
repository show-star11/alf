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
