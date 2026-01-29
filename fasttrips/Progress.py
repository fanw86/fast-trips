"""Progress tracking for Fast-Trips API integration."""
from __future__ import print_function

__copyright__ = "Copyright 2015 Contributing Entities"
__license__   = """
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""
import datetime
import json
import os
import tempfile
import time


class ProgressReporter(object):
    """Writes progress to JSON file using atomic write (temp + rename).

    This class provides progress updates during Fast-Trips assignment runs,
    enabling the API server to track and report real-time progress to clients.

    Two files are written:
    - ft_progress.json: Current state (overwritten each update) for API polling
    - ft_progress.jsonl: Incremental log with timestamps for profiling analysis
    """

    PROGRESS_FILE = "ft_progress.json"
    PROGRESS_LOG_FILE = "ft_progress.jsonl"

    def __init__(self, output_dir, enabled=True):
        """
        Constructor.

        :param output_dir: Location to write progress file
        :type output_dir:  string
        :param enabled:    Whether progress reporting is enabled
        :type enabled:     bool
        """
        self.output_dir = output_dir
        self.enabled = enabled
        self.progress_path = os.path.join(output_dir, self.PROGRESS_FILE) if output_dir else None
        self.progress_log_path = os.path.join(output_dir, self.PROGRESS_LOG_FILE) if output_dir else None
        # Store last known values to preserve them across partial updates
        self._last_num_passengers_arrived = 0
        self._last_num_bumped_passengers = 0
        self._last_total_demand = 0
        self._last_capacity_gap = None
        self._last_converged = None
        # Timing for profiling
        self._start_time = time.time()
        self._start_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._event_count = 0

    def update(self,
               phase,                      # "iteration_start", "pathfinding", "simulation_complete", "iteration_complete", "completed"
               iteration,
               max_iterations,
               pathfinding_iteration,
               paths_sought=0,
               total_paths=0,
               num_passengers_arrived=None,
               num_bumped_passengers=None,
               total_demand=None,
               capacity_gap=None,
               converged=None):
        """
        Write progress update to JSON file.

        Values for num_passengers_arrived, num_bumped_passengers, total_demand,
        capacity_gap, and converged are preserved from previous updates if not
        explicitly provided (None). This allows partial updates (e.g., during
        pathfinding) to retain the values from the last simulation phase.

        :param phase:                  Current phase of the assignment
        :type phase:                   string
        :param iteration:              Current outer iteration number
        :type iteration:               int
        :param max_iterations:         Maximum number of outer iterations
        :type max_iterations:          int
        :param pathfinding_iteration:  Current pathfinding iteration within the outer iteration
        :type pathfinding_iteration:   int
        :param paths_sought:           Number of paths sought so far
        :type paths_sought:            int
        :param total_paths:            Total number of paths to find
        :type total_paths:             int
        :param num_passengers_arrived: Number of passengers that arrived at destination (None to preserve last value)
        :type num_passengers_arrived:  int or None
        :param num_bumped_passengers:  Number of passengers bumped (capacity constrained) (None to preserve last value)
        :type num_bumped_passengers:   int or None
        :param total_demand:           Total number of passengers in demand (None to preserve last value)
        :type total_demand:            int or None
        :param capacity_gap:           Current capacity gap value (None to preserve last value)
        :type capacity_gap:            float or None
        :param converged:              Whether the assignment has converged (None to preserve last value)
        :type converged:               bool or None
        """
        if not self.enabled or not self.progress_path:
            return

        # Update stored values if explicitly provided, otherwise use last known values
        if num_passengers_arrived is not None:
            self._last_num_passengers_arrived = num_passengers_arrived
        if num_bumped_passengers is not None:
            self._last_num_bumped_passengers = num_bumped_passengers
        if total_demand is not None:
            self._last_total_demand = total_demand
        if capacity_gap is not None:
            self._last_capacity_gap = capacity_gap
        if converged is not None:
            self._last_converged = converged

        # Calculate timing
        current_time = time.time()
        elapsed_seconds = current_time - self._start_time
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._event_count += 1

        data = {
            "phase": phase,
            "iteration": iteration,
            "max_iterations": max_iterations,
            "pathfinding_iteration": pathfinding_iteration,
            "paths_sought": paths_sought,
            "total_paths": total_paths,
            "num_passengers_arrived": self._last_num_passengers_arrived,
            "num_bumped_passengers": self._last_num_bumped_passengers,
            "total_demand": self._last_total_demand,
            "capacity_gap": self._last_capacity_gap,
            "converged": self._last_converged,
        }

        # Data with profiling info for the log file
        log_data = data.copy()
        log_data["timestamp"] = timestamp
        log_data["elapsed_seconds"] = round(elapsed_seconds, 3)
        log_data["event_number"] = self._event_count

        try:
            os.makedirs(self.output_dir, exist_ok=True)

            # Write current state (atomic overwrite) for API polling
            fd, temp_path = tempfile.mkstemp(dir=self.output_dir, prefix=".ft_progress_", suffix=".tmp")
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f)
            os.replace(temp_path, self.progress_path)

            # Append to incremental log for profiling analysis
            if self.progress_log_path:
                with open(self.progress_log_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(log_data) + '\n')

        except (IOError, OSError):
            pass  # Best-effort - don't fail the run if progress update fails
