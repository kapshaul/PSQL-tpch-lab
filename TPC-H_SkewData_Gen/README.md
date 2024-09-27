## 1. Clone the dbgen directory.
`./dbgen -h`
The above command is the help function.

## 2. Inside dbgen folder, run the below command.
`./dbgen -s 10.0 -z 0`
The above command will create z = 0 data with 10GB size data.

## 3. Do the below command to see the first few rows of a table.
`head customer.tbl` If you observe carefully, each tuple will have a `|` symbol at the end.

For query loading and processing, the `|` at the end is not required. So, we need to remove this in the next step.

## 4. Remove the "|" at the end of the tuple in all the tables.
`sed -i 's/|$//' *.tbl`

After running the above command, we can see that `|` is removed for all the tuples in all the tables.

You can also run the script `remove.sh` for multiple tables.

## 5. Shuffle each table as below. If you do not shuffle, the tuples will be in sorted order.

Run the script `shuffle_tables.sh` for multiple tables.
