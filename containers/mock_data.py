import uuid

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

instances = []
context = "helloworld"
raise_delay_completed = False


class Endpoint(BaseModel):
    address: str
    region: str
    port: int


class Instance(BaseModel):
    name: str
    domains: list[str]
    endpoints: list[Endpoint]
    routes: list[str]
    service_clusters: list[str] = ["T1"]


@app.get("/context")
async def get_context():
    global raise_delay_completed
    if context == "raise" and not raise_delay_completed:
        # Simulate a slow/failing response on first attempt after context changes to "raise"
        raise_delay_completed = True
        raise ValueError("instructed to raise error (first attempt)")
    return context


@app.patch("/context/{new}")
async def patch_context(new):
    global context, raise_delay_completed
    context = new
    raise_delay_completed = False
    return ""


@app.get("/data")
async def get_data():
    return instances


@app.patch("/data")
async def set_data(
    number: int = 100,
    num_routes: int = 5,
    num_endpoints: int = 3,
    num_domains: int = 10,
):
    global instances
    routes = [str(uuid.uuid4()) for _ in range(num_routes)]
    domains = [f"{uuid.uuid4()}.internal" for _ in range(num_domains)]
    endpoints = [
        Endpoint(address=f"{uuid.uuid4()}.internal", region="foo", port=443)
        for _ in range(num_endpoints)
    ]
    instances = [
        Instance(
            name=str(uuid.uuid4()), domains=domains, endpoints=endpoints, routes=routes
        )
        for _ in range(number)
    ]
    return ""


@app.get("/health")
async def healthcheck():
    return "ok"
