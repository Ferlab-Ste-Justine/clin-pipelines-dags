clingen_gene_dosage_sensitivity = """#!/bin/bash

# A script to download and convert the ClinGen Gene Curation list (GRCh38)
# from TSV format to GFF3 format.
#
# GFF3 Format (9 columns, tab-separated):
# 1. seqid:   Chromosome name (e.g., 'chr1')
# 2. source:  Program/database that generated the feature (e.g., 'ClinGen')
# 3. type:    The type of the feature (e.g., 'gene')
# 4. start:   Start coordinate (1-based)
# 5. end:     End coordinate (1-based, inclusive)
# 6. score:   A score ('.' for not applicable)
# 7. strand:  Strand ('+', '-', or '.' for not applicable)
# 8. phase:   For CDS features ('0', '1', '2', or '.' for not applicable)
# 9. attributes: Semicolon-separated list of key=value pairs.

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error.
set -u
# The return value of a pipeline is the status of the last command to exit with a non-zero status.
set -o pipefail

# --- Configuration ---
OUTPUT_FILE="ClinGen_gene_curation_list_GRCh38"
URL="ftp.clinicalgenome.org/${OUTPUT_FILE}.tsv"

echo "### Starting ClinGen TSV to GFF3 conversion..."

# --- Main Logic ---
# 1. `curl -sL`: Download the file from the URL.
#    -s: silent mode (no progress meter)
#    -L: follow redirects
# 2. `grep -v '^#'`: Filter out the header lines that start with '#'.
# 3. `awk -v OFS='\t'`: The core of the conversion.
#    -v OFS='\t': Sets the Output Field Separator to a tab.
#    The AWK script is enclosed in single quotes.

curl -sL "${URL}" | grep -v '^#' | awk '
BEGIN {
    # Set the input field separator to a tab
    FS="\t";
    # Set the output field separator to a tab for GFF3 compliance
    OFS="\t";

    # Print the mandatory GFF3 header
    print "##gff-version 3";

    # Add a custom header to explain the source and attribute mapping
    print "#";
    print "# Converted from ClinGen Gene Curation List (GRCh38)";
    print "# Original file: ftp.clinicalgenome.org/ClinGen_gene_curation_list_GRCh38.tsv";
    print "#";
}
# Process every non-empty line
NF > 0 {
    # The input columns from the TSV file are:
    # $1: Gene Symbol
    # $2: Gene ID
    # $3: cytoBand
    # $4: Genomic Location (e.g., "chr22:42692121-42721301")
    # $5: Haploinsufficiency Score
    # $6: Haploinsufficiency Description
    # ...
    # $13: Triplosensitivity Score
    # ...
    # $21: Date Last Evaluated
    # $22: Haploinsufficiency Disease ID
    # $23: Triplosensitivity Disease ID

    # --- Parse required GFF3 columns ---

    # Column 4 (Genomic Location) needs to be split into seqid, start, and end
    split($4, location, /[:-]/);
    seqid = location[1];
    start = location[2];
    end   = location[3];

    # Basic validation: if genomic location is malformed, skip this line
    if (seqid == "" || start !~ /^[0-9]+$/ || end !~ /^[0-9]+$/) {
        # Print a warning to stderr and skip to the next line
        print "WARNING: Skipping line with invalid location data: " $0 > "/dev/stderr";
        next;
    }

    # --- Build the GFF3 attributes column (column 9) ---

    # Standard GFF3 attributes: ID and Name
    # ID must be unique. We will construct it from Gene Symbol and Gene ID.
    # Name is a common display name. Gene Symbol is perfect for this.
    attributes = "ID=" $1 "_" $2 ";Name=" $1;

    # Add other relevant TSV fields as custom attributes.
    # We replace spaces with underscores in descriptions for better compatibility.
    haplo_desc = $6; gsub(/ /, "_", haplo_desc);
    triplo_desc = $14; gsub(/ /, "_", triplo_desc);

    attributes = attributes ";gene_id=" $2;
    attributes = attributes ";cytoband=" $3;
    attributes = attributes ";haploinsufficiency_score=" $5;
    attributes = attributes ";haploinsufficiency_description=" haplo_desc;
    attributes = attributes ";triplosensitivity_score=" $13;
    attributes = attributes ";triplosensitivity_description=" triplo_desc;
    attributes = attributes ";date_last_evaluated=" $21;
    attributes = attributes ";haploinsufficiency_disease_id=" $22;
    attributes = attributes ";triplosensitivity_disease_id=" $23;

    # --- Print the final GFF3 line ---
    # col1: seqid      (e.g., chr22)
    # col2: source     ("ClinGen")
    # col3: type       ("gene")
    # col4: start      (e.g., 42692121)
    # col5: end        (e.g., 42721301)
    # col6: score      (".")
    # col7: strand     (".") - The source file does not provide strand info
    # col8: phase      (".") - Not a coding feature
    # col9: attributes (the string we built)
    print seqid, "ClinGen", "gene", start, end, ".", ".", ".", attributes;
}
' > "${OUTPUT_FILE}.gff3"

echo "### Success!"
echo "Converted file saved as: ${OUTPUT_FILE}.gff3"
echo
echo "### Example output from the new file:"
head -n 5 "${OUTPUT_FILE}.gff3"

(grep '^#' "${OUTPUT_FILE}.gff3"; grep -v '^#' "${OUTPUT_FILE}.gff3" | sort -k1,1 -k4,4n) > "${OUTPUT_FILE}.sorted.gff3"
"""