# Test parameters, the same string as in the test file format
NUM_NODES = "num_nodes"
TASKS_PER_ROUND = "tasks_per_rounds"
INPUT_DATA = "input_data"
OUTPUT_DATA = "output_data"
TEST_NAME = "name"
NUM_ROUNDS = "num_rounds"

DELAY = "delay"
TIMEOUT_PERIOD = "timeout"

input_slices_sources = ["LEFT", "RIGHT", "RANDOM"]
output_slices_sources = ["LEFT", "RIGHT", "SAME_AS_INPUT", "RANDOM", "SAME_LOC"]


# Tester messages

start_test_msg      = "**************** Start %s *****************"
end_test_msg        = "***************** End %s ******************"
test_errors_msg     = "Errors in iteration %d of %d:"
test_finished_msg   = "Test %-10s Finished...............%d%% completed"
test_bonus_msg      = " + %d%% bonus"
timout_msg          = "Test %-10s Timeout................%d%% completed"
