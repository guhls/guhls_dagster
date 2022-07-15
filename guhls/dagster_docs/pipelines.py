from dagster import job
from guhls.dagster_docs.solids import hello


@job
def job2():
    hello()


@job
def job3():
    hello()


@job
def job1():
    hello()
