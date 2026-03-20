#!/bin/bash

# Get the execution timestamp (Format: MMDDHHMMSS)
TIMESTAMP=$(date +"%m%d%H%M%S")

# Set the number of files per zip archive
BATCH_SIZE=10

# Define the output directory
OUTPUT_DIR="./archives"

# Create the output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Initialize variables
count=0
zip_index=1
files=()

echo "Scanning files and creating archives in $OUTPUT_DIR..."

# Use find to get all files (-type f).
# Exclude existing zip files (! -name "*.zip").
# Exclude the output directory itself (! -path "${OUTPUT_DIR}/*") to avoid infinite loops.
# Separate them with a null byte (-print0) to safely handle spaces in filenames.
while IFS= read -r -d '' file; do
    # Add the file to the array
    files+=("$file")
    ((count++))

    # Execute the zip command when the specified batch size is reached
    if (( count == BATCH_SIZE )); then
        # Include output path, timestamp, and sequential number in the zip filename
        zip_name="${OUTPUT_DIR}/archive_${TIMESTAMP}_$(printf "%03d" $zip_index).zip"
        
        # Execute zip command (-q for quiet mode)
        zip -q "$zip_name" "${files[@]}"
        echo "Created: $zip_name (containing $count files)"
        
        # Reset the counter and array for the next batch
        ((zip_index++))
        count=0
        files=()
    fi
done < <(find * -type f ! -name "*.zip" ! -path "${OUTPUT_DIR}/*" -print0)

# Process any remaining files that didn't make a full batch
if (( count > 0 )); then
    zip_name="${OUTPUT_DIR}/archive_${TIMESTAMP}_$(printf "%03d" $zip_index).zip"
    zip -q "$zip_name" "${files[@]}"
    echo "Created: $zip_name (containing $count files)"
fi

echo "All files have been successfully archived to $OUTPUT_DIR!"
