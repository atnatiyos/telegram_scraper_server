import schedule
import time
import subprocess

def run_script():
    subprocess.run(["python", "scrap.py"])

# Schedule the script to run every Monday at 10 AM
schedule.every(1).days.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
