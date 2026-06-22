from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from datetime import datetime
import random

# ShortCircuitOperator function - downstream tasks will be skipped if the function returns False
def shortcircuit_task():
    choice = [True, False, True, True]
    choosen_choice = random.choice(choice)
    print(f"ShortCircuit choice: {choosen_choice}")
    return choosen_choice

# BranchPythonOperator function - determines which branch to follow based on the returned value
def choose_branch():
    branches = ['branch_A', 'branch_B', 'branch_C']
    choosen_branch = random.choice(branches)
    print(f"Chosen branch: {choosen_branch}")
    return choosen_branch

def branch_A():
    arr = [8, 2, 5, 1, 4]
    sorted_arr = sorted(arr)
    return f"Branch A: Sorted array is {sorted_arr}"

def branch_B():
    arr = [8, 2, 5, 1, 4]
    reversed_arr = list(reversed(arr))
    return f"Branch B: Reversed array is {reversed_arr}"

def branch_C(sorted_arr, reversed_arr):
    try:
        if sorted_arr and reversed_arr:
            combined_arr = sorted_arr + reversed_arr
            return f"Branch C: Combined array is {combined_arr}"

    except ValueError as e:
        return f"Branch C: Error occurred - {e}"

with DAG(
    dag_id = "branching_and_circuit_dag",
    start_date = datetime(2026, 6, 21),
    schedule = '@daily',
    catchup = False,
    tags = ['example', 'branching', 'shortcircuit']
)as dag:
    
    task_shortcircuit = ShortCircuitOperator(
        task_id = 'shortcircuit_task',
        python_callable = shortcircuit_task
    )

    task_branch = BranchPythonOperator(
        task_id = 'choose_branch',
        python_callable = choose_branch
    )

    task_branch_A = PythonOperator(
        task_id = 'branch_A',
        python_callable = branch_A
    )
    
    task_branch_B = PythonOperator(
        task_id = 'branch_B',
        python_callable = branch_B
    )

    task_branch_C = PythonOperator(
        task_id = 'branch_C',
        python_callable = branch_C,
        op_kwargs = {
            "sorted_arr": [1, 2, 4, 5, 8],
            "reversed_arr": [4, 1, 5, 2, 8]
        }
    )

    task_shortcircuit >> task_branch
    task_branch >> [task_branch_A, task_branch_B, task_branch_C]