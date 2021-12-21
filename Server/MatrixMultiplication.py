def matrix_multiplication(x, y):
    return [[sum(a * b for a, b in zip(X_row, Y_col)) for Y_col in zip(*y)]
            for X_row in x]
