# Spark Application

Welcome to the Spark Application repository. This project serves as a comprehensive example of building and testing Spark Scala code on a local machine. It emphasizes the importance of producing quality, testable Spark code efficiently.

## Overview

This repository focuses on showcasing best practices for generating testable Spark/Scala code. The specific example involves creating product rankings for an e-commerce platform. The algorithm utilizes product performance and other metadata indicators as predictors for future rankings.

While demonstrating various Spark APIs (RDD, DataFrame, and Dataset), it's important to note that some APIs might not be optimal for all use cases. The primary goal is to provide a learning resource for producing high-quality, testable Spark Scala code.

## Running Tests
To run all tests, execute the following command:
```bash
sbt clean assembly
```
To run specific tests, use the following command:

```bash
sbt "testOnly spark_scala.merchant_page.MerchantPageRankingsTest"
```
