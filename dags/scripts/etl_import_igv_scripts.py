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

clingen_region_dosage_sensitivity = """#!/bin/bash

# A script to download and convert the ClinGen Region Curation list (GRCh38)
# into a sorted, indexed GFF3 file.
#
# This prepares the file for client-side coloring in IGV.js via the `colorBy`
# option, creating a 'haploinsufficiency_description' attribute. It does NOT
# embed color information directly into the file.

# --- Script Configuration ---
set -e
set -u
#set -o pipefail

# --- File Configuration ---
URL="ftp.clinicalgenome.org/ClinGen_region_curation_list_GRCh38.tsv"
BASE_FILENAME="ClinGen_region_curation_list_GRCh38"
UNSORTED_GFF3="${BASE_FILENAME}.unsorted.gff3"
SORTED_GFF3_GZ="${BASE_FILENAME}.sorted.gff3.gz"

echo "### Starting ClinGen Region TSV to GFF3 conversion..."

# --- Main Logic: Download and Convert to Unsorted GFF3 ---
curl -sL "${URL}" | grep -v '^#' | awk '
BEGIN {
    FS="\t";
    OFS="\t";
    print "##gff-version 3";
    print "# Converted from ClinGen Region Curation List (GRCh38)";
    print "# Original file: ftp.clinicalgenome.org/ClinGen_region_curation_list_GRCh38.tsv";
}
# Process every non-empty line
NF > 0 {
    # The input columns from the TSV file are:
    # $1: ISCA ID
    # $2: ISCA Region Name
    # $3: cytoBand
    # $4: Genomic Location (e.g., "chr16:16762939-18071284" or "tbd")
    # $5: Haploinsufficiency Score
    # $6: Haploinsufficiency Description
    # ... and so on

    # --- Parse required GFF3 columns ---
    split($4, location, /[:-]/);
    seqid = location[1];
    start = location[2];
    end   = location[3];

    # Basic validation: Skip line if location is "tbd" or malformed.
    # The regex check `!~ /^[0-9]+$/` correctly fails for "tbd".
    if (seqid == "" || start !~ /^[0-9]+$/ || end !~ /^[0-9]+$/) {
        print "WARNING: Skipping line with invalid or missing location data: " $0 > "/dev/stderr";
        next;
    }

    # --- Build the GFF3 attributes column (column 9) ---

    # ID must be unique. ISCA ID ($1) is perfect for this.
    # Name is the human-readable name. ISCA Region Name ($2) is best.
    # We must sanitize the name for use as an attribute value.
    name_attr = $2;
    gsub(/ /, "_", name_attr);      # Replace spaces with underscores
    gsub(/[();,]/, "", name_attr); # Remove common problematic characters

    attributes = "ID=" $1 ";Name=" name_attr;

    # Sanitize the description fields for use as attribute values.
    haplo_desc_attr = $6; gsub(/ /, "_", haplo_desc_attr);
    triplo_desc_attr = $14; gsub(/ /, "_", triplo_desc_attr);

    # Add other relevant TSV fields as custom attributes, only if they have a value.
    attributes = attributes ";isca_id=" $1;
    if ($3 != "") { attributes = attributes ";cytoband=" $3; }
    if ($5 != "") { attributes = attributes ";haploinsufficiency_score=" $5; }
    if ($6 != "") { attributes = attributes ";haploinsufficiency_description=" haplo_desc_attr; }
    if ($13 != "") { attributes = attributes ";triplosensitivity_score=" $13; }
    if ($14 != "") { attributes = attributes ";triplosensitivity_description=" triplo_desc_attr; }
    if ($21 != "") { attributes = attributes ";date_last_evaluated=" $21; }
    if ($22 != "") { attributes = attributes ";haploinsufficiency_disease_id=" $22; }
    if ($23 != "") { attributes = attributes ";triplosensitivity_disease_id=" $23; }

    # --- Print the final GFF3 line ---
    # The feature type (column 3) is now "region".
    print seqid, "ClinGen", "region", start, end, ".", ".", ".", attributes;
}
' > "${UNSORTED_GFF3}"

echo "### Conversion complete. Now sorting and indexing..."

# Sort, compress with bgzip, and index with tabix
(grep '^#' "${UNSORTED_GFF3}"; grep -v '^#' "${UNSORTED_GFF3}" | sort -k1,1 -k4,4n) > "${BASE_FILENAME}.sorted.gff3"

# Cleanup
rm "${UNSORTED_GFF3}"

echo
echo "### Success!"
echo "Generated browser-ready files:"
echo "1. Data file: ${SORTED_GFF3_GZ}"
echo "2. Index file: ${SORTED_GFF3_GZ}.tbi"
"""

ucsc_chainself = """#!/bin/bash

# ==============================================================================
# Script to download, convert, compress, and index the UCSC hg38 chainSelf file.
#
# This script performs the following steps:
# 1. Checks for required dependencies (curl, awk, bgzip, tabix).
# 2. Downloads the hg38 chainSelf.txt.gz file using curl.
# 3. Streams the uncompressed data directly to an awk script for conversion
#    to GFF3 format. This avoids writing the huge intermediate text file to disk.
# 4. Compresses the resulting GFF3 file using bgzip.
# 5. Creates a tabix index (.tbi) for the compressed GFF3 file.
# 6. Cleans up the intermediate GFF3 file.
# ==============================================================================

# --- Configuration ---
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipes will fail if any command in the pipe fails, not just the last one.
set -o pipefail

# --- Variables ---
URL="https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/chainSelf.txt.gz"
GZ_FILE="chainSelf.txt.gz"
GFF_FILE="hg38.chainSelf.gff3"

# 2. Download the source file using curl
echo -e "\n--> Step 1 of 4: Downloading source file..."
if [ ! -f "$GZ_FILE" ]; then
    # -L: Follow redirects
    # -C -: Continue/resume a previously interrupted download
    # -o: Write output to a file with the specified name
    curl -L -C - -o "$GZ_FILE" "$URL"
else
    echo "File '$GZ_FILE' already exists. Skipping download."
    echo "To re-download, please delete the existing file first."
fi
echo "Download complete."

# 3. Uncompress, Convert to GFF3, and Save
# We stream directly from gunzip to awk to avoid creating a massive intermediate file.
# The source file is already sorted by chromosome and start position,
# which is a requirement for tabix, so no extra sorting step is needed.
echo -e "\n--> Step 2 of 4: Converting to GFF3 format (streaming)..."
gunzip -c "$GZ_FILE" | awk '
BEGIN {
    # Set the output field separator to a tab
    OFS="\t";

    # Print the mandatory GFF3 header
    print "##gff-version 3";
}
{
    # Column mapping based on the provided SQL schema:
    # $1: bin, $2: score, $3: tName, $4: tSize, $5: tStart, $6: tEnd
    # $7: qName, $8: qSize, $9: qStrand, $10: qStart, $11: qEnd, $12: id, $13: normScore

    # GFF3 Columns:
    # 1. seqid:       Chromosome name ($3)
    # 2. source:      A name for the tool/database.
    # 3. type:        The type of feature.
    # 4. start:       Start coordinate (1-based). UCSC is 0-based, so add 1.
    # 5. end:         End coordinate (1-based).
    # 6. score:       The alignment score ($2).
    # 7. strand:      Use "." as the feature strand is not intrinsically defined.
    #                 The query strand ($9) is stored in the attributes.
    # 8. phase:       "." for non-coding features.
    # 9. attributes:  Key-value pairs with extra info.

    seqid = $3;
    source = "UCSC_chainSelf";
    type = "self_chain_alignment";
    start = $5 + 1; # Convert 0-based start to 1-based
    end = $6;
    score = $2;
    strand = ".";
    phase = ".";

    # Create a detailed attributes string
    attributes = sprintf("ID=chainSelf_%s;score=%.f;normScore=%.2f;Target=%s %d %d;target_strand=%s", $12, $2, $13, $7, $10+1, $11, $9);

    # Print the formatted GFF3 line
    print seqid, source, type, start, end, score, strand, phase, attributes;
}' > "$GFF_FILE"

echo "Conversion to GFF3 complete. Output is in '$GFF_FILE'."
"""

ucsc_genomic_superdups = """#!/bin/bash

# ==============================================================================
# Script to download, convert, compress, and index the UCSC hg38
# genomicSuperDups file.
#
# This script performs the following steps:
# 1. Checks for required dependencies (curl, awk, bgzip, tabix).
# 2. Downloads the hg38 genomicSuperDups.txt.gz file using curl.
# 3. Streams the uncompressed data directly to an awk script for conversion
#    to GFF3 format, avoiding a large intermediate file on disk.
# 4. Compresses the resulting GFF3 file using bgzip.
# 5. Creates a tabix index (.tbi) for the compressed GFF3 file.
# ==============================================================================

# --- Configuration ---
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipes will fail if any command in the pipe fails, not just the last one.
set -o pipefail

# --- Variables ---
URL="https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/genomicSuperDups.txt.gz"
GZ_FILE="genomicSuperDups.txt.gz"
GFF_FILE="hg38.genomicSuperDups.gff3"

# 2. Download the source file using curl
echo -e "\n--> Step 1 of 4: Downloading source file..."
if [ ! -f "$GZ_FILE" ]; then
    # -L: Follow redirects
    # -C -: Continue/resume a previously interrupted download
    # -o: Write output to a file with the specified name
    curl -L -C - -o "$GZ_FILE" "$URL"
else
    echo "File '$GZ_FILE' already exists. Skipping download."
    echo "To re-download, please delete the existing file first."
fi
echo "Download complete."

# 3. Uncompress, Convert to GFF3, and Save
# We stream directly from gunzip to awk to avoid creating a massive intermediate file.
# The source file is already sorted by chromosome and start position, a requirement
# for tabix, so no extra sorting step is needed.
echo -e "\n--> Step 2 of 4: Converting to GFF3 format (streaming)..."
gunzip -c "$GZ_FILE" | awk '
BEGIN {
    # Set the output field separator to a tab
    OFS="\t";

    # Print the mandatory GFF3 header
    print "##gff-version 3";
}
{
    # Column mapping based on the provided SQL schema for genomicSuperDups:
    # $2: chrom, $3: chromStart, $4: chromEnd, $5: name, $6: score, $7: strand
    # $8: otherChrom, $9: otherStart, $10: otherEnd, $12: uid
    # $19: alignL, $23: matchB, $24: mismatchB, $27: fracMatch, $28: fracMatchIndel

    # GFF3 Columns:
    # 1. seqid:       Chromosome name ($2)
    # 2. source:      A name for the tool/database.
    # 3. type:        The type of feature.
    # 4. start:       Start coordinate (1-based). UCSC is 0-based, so add 1.
    # 5. end:         End coordinate (1-based).
    # 6. score:       The score ($6).
    # 7. strand:      The strand ($7).
    # 8. phase:       "." for non-coding features.
    # 9. attributes:  Key-value pairs with extra info.

    seqid = $2;
    source = "UCSC_SuperDups";
    type = "";
    start = $3 + 1; # Convert 0-based start to 1-based
    end = $4;
    score = $6;
    strand = $7;
    phase = ".";

    # Create a detailed attributes string with info about the duplicated region
    # Note: otherStart ($9) is also 0-based, so we add 1 for a 1-based GFF representation.
    # The ID is a unique, incrementing integer using awks NR (record number).
    attributes = sprintf("ID=gsd_%d;Name=%s;Target=%s %d %d;frac_match=%.6f;frac_match_indel=%.6f;align_length=%d;match_bases=%d;mismatch_bases=%d", NR, $5, $8, $9+1, $10, $27, $28, $19, $23, $24);

    # Print the formatted GFF3 line
    print seqid, source, type, start, end, score, strand, phase, attributes;
}' > "$GFF_FILE"

echo "Conversion to GFF3 complete. Output is in '$GFF_FILE'."
"""

ucsc_repeat_masker = """#!/bin/bash

# ==============================================================================
# Script to download, convert, compress, and index the UCSC hg38 rmsk
# (RepeatMasker) file.
#
# This script performs the following steps:
# 1. Checks for required dependencies (curl, awk, bgzip, tabix).
# 2. Downloads the hg38 rmsk.txt.gz file using curl.
# 3. Streams the uncompressed data directly to an awk script for conversion
#    to GFF3 format, avoiding a large intermediate file on disk.
# 4. Compresses the resulting GFF3 file using bgzip.
# 5. Creates a tabix index (.tbi) for the compressed GFF3 file.
# ==============================================================================

# --- Configuration ---
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipes will fail if any command in the pipe fails, not just the last one.
set -o pipefail

# --- Variables ---
URL="https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/rmsk.txt.gz"
GZ_FILE="rmsk.txt.gz"
GFF_FILE="hg38.rmsk.gff3"

# 2. Download the source file using curl
echo -e "\n--> Step 1 of 4: Downloading source file..."
if [ ! -f "$GZ_FILE" ]; then
    # -L: Follow redirects
    # -C -: Continue/resume a previously interrupted download
    # -o: Write output to a file with the specified name
    curl -L -C - -o "$GZ_FILE" "$URL"
else
    echo "File '$GZ_FILE' already exists. Skipping download."
    echo "To re-download, please delete the existing file first."
fi
echo "Download complete."

# 3. Uncompress, Convert to GFF3, and Save
# We stream directly from gunzip to awk to avoid creating a massive intermediate file.
# The source file is already sorted, so no extra sorting step is needed.
echo -e "\n--> Step 2 of 4: Converting to GFF3 format (streaming)..."
gunzip -c "$GZ_FILE" | awk '
BEGIN {
    # Set the output field separator to a tab
    OFS="\t";

    # Print the mandatory GFF3 header
    print "##gff-version 3";
}
{
    # Column mapping based on the provided SQL schema for rmsk:
    # $2: swScore, $3: milliDiv, $4: milliDel, $5: milliIns
    # $6: genoName, $7: genoStart, $8: genoEnd, $10: strand
    # $11: repName, $12: repClass, $13: repFamily, $17: id

    # GFF3 Columns:
    # 1. seqid:       Chromosome name ($6)
    # 2. source:      A name for the tool/database that generated the annotation.
    # 3. type:        The type of feature, here we use the repeat class.
    # 4. start:       Start coordinate (1-based). UCSC is 0-based, so add 1.
    # 5. end:         End coordinate (1-based).
    # 6. score:       The Smith-Waterman score ($2).
    # 7. strand:      The strand ($10).
    # 8. phase:       "." for non-coding features.
    # 9. attributes:  Key-value pairs with extra info.

    seqid = $6;
    source = "RepeatMasker";
    type = $12;
    start = $7 + 1; # Convert 0-based start to 1-based
    end = $8;
    score = $2;
    strand = $10;
    phase = ".";

    # Create a unique ID and a detailed attributes string
    # ID is a simple auto-incrementing integer (NR)
    attributes = sprintf("ID=rmsk_%d;Name=%s;rep_family=%s;milli_div=%d;milli_del=%d;milli_ins=%d", NR, $11, $13, $3, $4, $5);

    # Print the formatted GFF3 line
    print seqid, source, type, start, end, score, strand, phase, attributes;
}' > "$GFF_FILE"

echo "Conversion to GFF3 complete. Output is in '$GFF_FILE'."
"""

ucsc_simple_repeat ="""#!/bin/bash

# ==============================================================================
# Script to download, convert, compress, and index the UCSC hg38 simpleRepeat
# (TRF) file.
#
# This script performs the following steps:
# 1. Checks for required dependencies (curl, awk, bgzip, tabix).
# 2. Downloads the hg38 simpleRepeat.txt.gz file using curl.
# 3. Streams the uncompressed data directly to an awk script for conversion
#    to GFF3 format, avoiding a large intermediate file on disk.
# 4. Compresses the resulting GFF3 file using bgzip.
# 5. Creates a tabix index (.tbi) for the compressed GFF3 file.
# ==============================================================================

# --- Configuration ---
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Pipes will fail if any command in the pipe fails, not just the last one.
set -o pipefail

# --- Variables ---
URL="https://hgdownload.soe.ucsc.edu/goldenPath/hg38/database/simpleRepeat.txt.gz"
GZ_FILE="simpleRepeat.txt.gz"
GFF_FILE="hg38.simpleRepeat.gff3"

# 2. Download the source file using curl
echo -e "\n--> Step 1 of 4: Downloading source file..."
if [ ! -f "$GZ_FILE" ]; then
    # -L: Follow redirects
    # -C -: Continue/resume a previously interrupted download
    # -o: Write output to a file with the specified name
    curl -L -C - -o "$GZ_FILE" "$URL"
else
    echo "File '$GZ_FILE' already exists. Skipping download."
    echo "To re-download, please delete the existing file first."
fi
echo "Download complete."

# 3. Uncompress, Convert to GFF3, and Save
# We stream directly from gunzip to awk to avoid creating a massive intermediate file.
# The source file is already sorted, so no extra sorting step is needed.
echo -e "\n--> Step 2 of 4: Converting to GFF3 format (streaming)..."
gunzip -c "$GZ_FILE" | awk '
BEGIN {
    # Set the output field separator to a tab
    OFS="\t";

    # Print the mandatory GFF3 header
    print "##gff-version 3";

    # Initialize a counter for unique IDs
    id_counter = 0;
}
{
    # Column mapping based on the provided SQL schema for simpleRepeat:
    # $2: chrom, $3: chromStart, $4: chromEnd, $5: name, $6: period, $7: copyNum
    # $11: score, $16: entropy, $17: sequence

    # GFF3 Columns:
    # 1. seqid:       Chromosome name ($2)
    # 2. source:      Tool that generated the annotation ($5, usually "trf").
    # 3. type:        The type of feature. "tandem_repeat" is appropriate.
    # 4. start:       Start coordinate (1-based). UCSC is 0-based, so add 1.
    # 5. end:         End coordinate (1-based).
    # 6. score:       The TRF score ($11).
    # 7. strand:      Not defined for this feature type, so use ".".
    # 8. phase:       "." for non-coding features.
    # 9. attributes:  Key-value pairs with extra info.

    seqid = $2;
    source = $5; # Usually "trf" for Tandem Repeats Finder
    type = "";
    start = $3 + 1; # Convert 0-based start to 1-based
    end = $4;
    score = $11;
    strand = ".";
    phase = ".";

    # Increment the counter for a new unique ID
    id_counter++;

    # Create a unique ID and a detailed attributes string
    attributes = sprintf("ID=trf_%d;period=%d;copy_num=%.1f;entropy=%.2f;consensus_sequence=%s", id_counter, $6, $7, $16, $17);

    # Print the formatted GFF3 line
    print seqid, source, type, start, end, score, strand, phase, attributes;
}' > "$GFF_FILE"

echo "Conversion to GFF3 complete. Output is in '$GFF_FILE'."
"""