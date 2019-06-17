import csv
import sys

def readRows(inputCSV):
    with open (inputCSV, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield (row)

def writeRows(output_csv):
    with open (output_csv, 'w') as op:
        fieldnames = ['Product ID', 'Customer Count', 'Total Revenue']
        writer = csv.DictWriter(op, fieldnames = fieldnames)
        
        writer.writeheader()
        for i in sorted(OutputDict.keys()):
            writer.writerow({'Product ID':i, 'Customer Count': OutputDict[i][1], 
                              'Total Revenue':OutputDict[i][0]})
        
if __name__ == '__main__':
    OutputDict = {}
    TemProductID = set([]) 
    CurrentCustomer = None
    
    inputCSV = sys.argv[1]
    outputCSV = sys.argv[2]

    
    for row in readRows(inputCSV):
        if CurrentCustomer != row['Customer ID']:
            CurrentCustomer = row['Customer ID']
            TemProductID = set([])
            
        if row['Product ID'] not in list(OutputDict.keys()):
            OutputDict[row['Product ID']] = [float(row['Item Cost'])]
            OutputDict[row['Product ID']].append(1)
            TemProductID.add(row['Product ID'])
        else: 
            OutputDict[row['Product ID']][0] += float(row['Item Cost'])
            if row['Product ID'] not in  TemProductID:
                OutputDict[row['Product ID']][1] += 1
                TemProductID.add(row['Product ID'])

    writeRows(outputCSV)
    