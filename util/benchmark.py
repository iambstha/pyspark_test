import time
import functools

class Benchmark:

    def __init__(self):
        self.records = []

    def measure(self, operation_name: str):
        return self.Timer(self, operation_name)

    def record(self, operation_name: str, elapsed_time: float):
        self.records.append({"operation": operation_name, "time": elapsed_time})
        print(f"⏱️   {operation_name} took {elapsed_time:.4f} seconds.")

    def summary(self):
        print("\n📊 Benchmark Summary:")
        for record in self.records:
            print(f"🕒   {record['operation']}: {record['time']:.4f} seconds")

    class Timer:
        def __init__(self, benchmark, operation_name):
            self.benchmark = benchmark
            self.operation_name = operation_name

        def __enter__(self):
            self.start_time = time.perf_counter()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            elapsed_time = time.perf_counter() - self.start_time
            self.benchmark.record(self.operation_name, elapsed_time)

    def benchmark_decorator(self, operation_name: str):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                result = func(*args, **kwargs)
                elapsed_time = time.perf_counter() - start_time
                self.record(operation_name, elapsed_time)
                return result
            return wrapper
        return decorator
