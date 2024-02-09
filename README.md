# Matrix Multiplication and Addition

This repository contains Java code for performing matrix multiplication and addition using Hadoop MapReduce.

## Introduction

The code provided implements two MapReduce jobs:
1. Multiplication: This job reads two matrices stored in separate files and multiplies them using MapReduce.
2. Addition: This job reads the result of matrix multiplication and performs addition to get the final result.

## Requirements

- Java Development Kit (JDK) version 8 or higher
- Apache Hadoop installed and configured

## Input Format

- The input matrices are assumed to be in CSV format.
- Each line represents a matrix element in the format: `i, j, value`, where `i` is the row index, `j` is the column index, and `value` is the matrix element value.

## Output Format

- The output of the multiplication job is stored in the `intermediate-output` directory.
- The output of the addition job is stored in the `final-output` directory.
- Each line in the output files represents a matrix element in the format: `i, j, value`, where `i` is the row index, `j` is the column index, and `value` is the matrix element value.
