# from pyspark.shell import sc
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import csv
import os
import argparse

number_of_columns = 0


def make_candidate_matrix(x):
    matrix = []
    for i in range(0, number_of_columns):
        matrix.append([True] * number_of_columns)
    include_x = [False] * number_of_columns
    for column in list(x):
        include_x[column] = True

    for i in range(0, number_of_columns):
        for j in range(0, number_of_columns):
            if include_x[i] and not include_x[j]:
                matrix[i][j] = False
    return matrix


def make_containment_relationships(x):
    include_x = [False] * number_of_columns
    for column in list(x):
        include_x[column] = True
    return include_x


def matrix_and(x, y):
    z = []
    for i in range(0, number_of_columns):
        z.append([False] * number_of_columns)
    for i in range(0, number_of_columns):
        for j in range(0, number_of_columns):
            z[i][j] = x[i][j] and y[i][j]
    return z


def sum_func(accum, n):
    return accum + n


def main():
    parser = argparse.ArgumentParser(description="Find Dependency inclusions")
    parser.add_argument('--path', type=str)
    parser.add_argument('--cores', type=str)
    args = parser.parse_args()

    sc = SparkContext(appName="DDM")
    sc.getConf().set("spark.executor.cores", args.cores)
    sc.getConf().set("spark.driver.cores", args.cores)
    sc.getConf().set("spark.worker.cores", args.cores)
    sc.getConf().set("spark.deploy.defaultCores", args.cores)
    sc.getConf().set("spark.driver.memory", "15g")
    global number_of_columns
    data = []
    file_headers = []
    for file in os.listdir(args.path):
        if file.endswith(".csv"):
            rdd = sc.textFile(os.path.join(args.path, file)).map(lambda line: line[1:-1].split("\";\""))

            file_data = rdd.collect()
            file_header = file_data[0]
            del file_data[0]
            file_data = [(number_of_columns, x) for x in file_data]
            data += file_data
            file_headers += file_header
            number_of_columns = number_of_columns + len(file_header)

    header_dummies = list(range(0, number_of_columns))
    rdd = sc.parallelize(data)
    values_as_key = rdd.flatMap(lambda el: list(zip(el[1], range(el[0], el[0] + len(el[1])))))
    unique_values = values_as_key.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)
    unique_values = unique_values.map(lambda x: (tuple(x[1]), 0)).reduceByKey(sum_func)
    matrix_per_key = unique_values.map(lambda x: make_candidate_matrix(x[0]))
    result_matrix = matrix_per_key.reduce(lambda x, y: matrix_and(x, y))

    assert len(result_matrix) == number_of_columns

    output = []
    for i in range(0, number_of_columns):
        assert len(result_matrix[i]) == number_of_columns
        output.append([])

    for i in range(0, len(result_matrix)):
        for j in range(0, len(result_matrix[i])):
            if i != j and result_matrix[i][j]:
                output[j].append(file_headers[i])

    for i in range(0, len(output)):
        row = output[i]
        if len(row) != 0:
            output_string = str(row[0])
            for j in range(1, len(row)):
                output_string += (", " + str(row[j]))
            print(str(file_headers[i]) + " < " + output_string)

    sc.stop()


if __name__ == "__main__":
    main()
