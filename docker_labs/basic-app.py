import numpy as np
import os

name = os.environ.get("NAME", "world")
print(f"Hello, {name}!")

# Create a simple array
arr = np.array([1, 2, 3, 4, 5])
print(f"Array: {arr}")
print(f"Mean: {arr.mean()}")
print(f"Sum: {arr.sum()}")

# Matrix multiplication
a = np.array([[1, 2], [3, 4]])
b = np.array([[5, 6], [7, 8]])
result = np.dot(a, b)
print(f"\nMatrix A:\n{a}")
print(f"Matrix B:\n{b}")
print(f"A dot B:\n{result}")
