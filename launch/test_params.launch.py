import os

from launch_ros.actions import Node

from launch import LaunchDescription
from launch.frontend.parse_substitution import get_package_share_directory


def generate_launch_description():
    config = os.path.join(
        get_package_share_directory("ros2_prometheus"), "config", "params.yaml"
    )

    return LaunchDescription(
        [
            Node(
                package="ros2_prometheus",
                executable="ros2_prometheus",
                name="ros2_prometheus",
                parameters=[config],
                output="screen",
                emulate_tty=True,
            )
        ]
    )
