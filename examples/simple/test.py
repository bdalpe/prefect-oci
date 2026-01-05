from prefect import flow

@flow(log_prints=True)
def hello_world():
    print("Hello, world!")