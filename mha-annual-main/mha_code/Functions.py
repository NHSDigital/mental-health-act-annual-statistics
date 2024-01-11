# Databricks notebook source
def suppress(x, base=5):   
    if x < 5:
        return '*'
    else:
        return int(base * round(float(x)/base))
spark.udf.register("suppress", suppress)