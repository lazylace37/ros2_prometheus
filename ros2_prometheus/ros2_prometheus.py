from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Any, List, Mapping, Optional

import builtin_interfaces
import rclpy
from rcl_interfaces.msg import ParameterDescriptor
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.time import Time
from ros2topic.verb.echo import get_msg_class
from rosidl_runtime_py.convert import rosidl_parser


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        response = self.server.parent.get_metrics()

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(response.encode("utf-8"))


@dataclass
class TopicMessage:
    time: int
    value: Any


class Ros2Prometheus(Node):
    _httpd: HTTPServer
    _httpd_thread: Thread

    _pending_topics: List[str] = []
    metrics: Mapping[str, Optional[TopicMessage]] = {}

    def __init__(self) -> None:
        super().__init__("ros2_prometheus")
        self.context.on_shutdown(self._on_shutdown)

        self.declare_parameter(
            "port", 8000, ParameterDescriptor(type=Parameter.Type.INTEGER)
        )
        port = self.get_parameter("port").value

        self._httpd = HTTPServer(("", port), RequestHandler)
        self._httpd.parent = self
        self.get_logger().info(f"Serving on {self._httpd.server_address}")
        self._httpd_thread = Thread(target=self._httpd.serve_forever, daemon=True)

        self.declare_parameters(
            namespace="",
            parameters=[
                ("topics", Parameter.Type.STRING_ARRAY),
            ],
        )
        topic_names = self.get_parameter("topics")
        self.get_logger().info("Loading topics: " + ",".join(topic_names.value))

        for topic_name in topic_names.value:
            self.metrics[topic_name] = None

            # self.declare_parameters(
            #     namespace="",
            #     parameters=[(f"{topic_name}.rate", Parameter.Type.INTEGER)],
            # )

            self._pending_topics.append(topic_name)

        self.timer = self.create_timer(1.0, self._timer_callback)
        self._httpd_thread.start()

    def _timer_callback(self):
        for topic_name in self._pending_topics:
            message_type = get_msg_class(self, topic_name, include_hidden_topics=True)
            if message_type is not None:
                self.create_subscription(
                    message_type,
                    topic_name,
                    lambda m, i, topic_name=topic_name: self._subscriber_callback(
                        topic_name, m, i
                    ),
                    10,
                    event_callbacks=None,
                )

                self.get_logger().info(f"Subscribed to {topic_name}")
                self._pending_topics.remove(topic_name)
        if len(self._pending_topics) == 0:
            self.timer.cancel()

    def _metrics_from_message(self, name: str, time: int, message: Any) -> str:
        res = ""

        for s, t in zip(
            message.get_fields_and_field_types().keys(), message.SLOT_TYPES
        ):
            field = getattr(message, s)
            if isinstance(field, builtin_interfaces.msg.Time):
                res += f"# TYPE {name} gauge\n"
                res += f'{name}{{field="{s}"}} {field.sec}.{field.nanosec} {time}\n'
            elif isinstance(t, rosidl_parser.definition.AbstractGenericString):
                res += f"# TYPE {name} gauge\n"
                res += f'{name}{{field="{s}",value="{field}"}} 1 {time}\n'
            elif isinstance(t, rosidl_parser.definition.BasicType):
                res += f"# TYPE {name} gauge\n"
                res += f'{name}{{field="{s}"}} {field} {time}\n'
            elif isinstance(t, rosidl_parser.definition.AbstractSequence):
                for i, v in enumerate(field):
                    res += self._metrics_from_message(name + f"_{s}_{i}", time, v)
            else:
                res += self._metrics_from_message(name + "_" + s, time, field)
        return res

    def get_metrics(self) -> str:
        res = ""
        for topic_name, topic_message in self.metrics.items():
            if topic_message is None:
                continue
            if topic_name.startswith("/"):
                topic_name = topic_name[1:]
            topic_name = topic_name.replace("/", "_")

            res += self._metrics_from_message(
                topic_name, topic_message.time, topic_message.value
            )
        return res

    def _subscriber_callback(self, topic_name, message, info):
        time = info["source_timestamp"] // 1_000_000
        if hasattr(message, "header"):
            header_time = Time.from_msg(message.header.stamp).nanoseconds // 1_000_000
            if header_time > 0:
                time = header_time

        self.metrics[topic_name] = TopicMessage(
            time=time,
            value=message,
        )

    def _on_shutdown(self):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._httpd_thread.join()


def main(args=None):
    rclpy.init(args=args)

    ros2_prometheus = Ros2Prometheus()
    rclpy.spin(ros2_prometheus)

    ros2_prometheus.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
