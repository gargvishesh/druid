import check_test_suite
import sys

if __name__ == '__main__':
    suite_name = ""

    # run this script with 2 arguments i.e. <test-suite-name> <comma_separatedall_changed_files>
    # variables. if it doesn't exist, fail
    if len(sys.argv) == 3:
        suite_name = sys.argv[1]
        all_changed_files_string = sys.argv[2]
    else:
        sys.stderr.write("usage: check_test_suite.py <test-suite-name> <all_changed_files>\n")
        sys.stderr.write("  e.g., check_test_suite.py docs 'docs/ingestion/index.md'")
        sys.exit(1)

    all_changed_files = all_changed_files_string.split(",")

    # Return if the test should run or not
    print(check_test_suite.check_should_run_suite(suite_name, all_changed_files))
