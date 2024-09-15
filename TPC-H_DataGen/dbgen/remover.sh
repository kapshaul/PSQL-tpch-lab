
path="/data/leeyongh-psql/Data/dbgen/z0"

tables=("customer" "lineitem" "nation" "order" "part" "partsupp" "region" "supplier")

for table in "${tables[@]}"; do
    sed -i 's/|$//' "$path/$table.tbl"
done
