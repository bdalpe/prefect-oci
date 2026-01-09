from prefect import flow, task
import requests

@task
def get_url(url: str):
    return requests.get(url).status_code

@flow
def simple_flow(url: str = "https://www.google.com"):
    status_code = get_url(url)
    print(f"Status code: {status_code}")
    return status_code

if __name__ == "__main__":
    simple_flow()
