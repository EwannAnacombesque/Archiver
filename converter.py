import pandas as pd
"""
Turns txt data.gouv files to xlsx ones
"""
filename = "Data/lég2017_all.txt"

data = open(filename,"r",encoding="iso-8859-1")
# Get the rows of the file, and turn int to int, float to float
rows = [[int(cell) if cell.isdigit() else float(cell.replace(",",".")) if cell.replace(",","",1).isdigit() else cell for cell in line.replace("\n","").split(";")] for line in data.readlines()]

# Get the longest row
max_row_length = len(max(rows,key=lambda x:len(x)))

# Make sure all rows are the same size (max row one's)
rectified_rows = [row + [None]*(max_row_length-len(row)) for row in rows]
# Change first row names <=> columns to have unique names
rectified_rows[0] = [rectified_rows[0][i] if rectified_rows[0][i] != None else str(i) for i in range(max_row_length)]

# EXPORT THE DATA SET #

dataset = pd.DataFrame(rectified_rows[1:], columns=[rectified_rows[0]])

with pd.ExcelWriter('Data/lég2017_all.xlsx') as writer:
    dataset.to_excel(writer)