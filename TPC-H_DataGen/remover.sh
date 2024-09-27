
path="/data/leeyongh-psql/Data/TPCH_10GB/z1_5"

tables=("customer" "lineitem" "nation" "order" "part" "partsupp" "region" "supplier")

for table in "${tables[@]}"; do
    sed -i 's/|$//' "$path/$table.tbl"
done
