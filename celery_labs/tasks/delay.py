from celery import shared_task
import time
import numpy as np

@shared_task
def delay_task(seconds):
    # Generate a random list length between 1 and 100
    length = np.random.randint(1, 100)
    
    # Generate a list of random integers between 1 and 1000 of size 'length'
    l = np.random.randint(1, 1000, size=length).tolist()
    
    # Sort the list
    l.sort()
    
    print(l)
    print(f"Task will sleep for {seconds} seconds")
    time.sleep(seconds)
    print(f"Task woke up after {seconds} seconds")
    return f"Slept for {seconds} seconds"
