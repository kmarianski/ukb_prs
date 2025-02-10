#!/bin/bash

path="/mnt/project/Bulk/Exome sequences/Population level exome OQFE variants, PLINK format - final release/"

file_prefix="ukb23158_c"

output_dir="./output"


mkdir -p "$output_dir"


for chr in 3 7 16 17; do

    plink_file="${path}${file_prefix}${chr}_b0_v1"

    ./plink \

        --bfile "$plink_file" \

        --extract variant_list.txt \

        --recodeA \

        --out "${output_dir}/dosage_chr${chr}"

done