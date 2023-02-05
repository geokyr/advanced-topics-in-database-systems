def print_dataframe(list_of_rows, file):

    # Extract the column names from the result
    columns = list_of_rows[0].__fields__

    # Print the header row
    header = "|" + "|".join(columns) + "|"
    separator = "|" + "|".join(["---"] * len(columns)) + "|"
    print(header, file=file)
    print(separator, file=file)

    # Print the values of the result rows
    for row in list_of_rows:
        row_values = [str(row[col]) for col in columns]
        row_string = "|" + "|".join(row_values) + "|"
        print(row_string, file=file)