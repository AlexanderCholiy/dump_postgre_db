import sys
import time
import subprocess
import threading

import schedule


def run_script(script_name):
    subprocess.run([sys.executable, script_name])


def schedule_script(script_name, times):
    for time_str in times:
        schedule.every().day.at(time_str).do(
            lambda script=script_name: threading.Thread(
                target=run_script, args=(script,)
            ).start()
        )


def main():
    schedule_script('run_dump_db.py', ['21:00'])

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
