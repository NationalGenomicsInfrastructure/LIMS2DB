# LIMS2DB Version Log

## 20241025.1

Send emails on contract update for RNA-seq (single cell) projects.

## 20241003.1

Update AVITI step IDs

## 20240912.1

Update project type list for mail_resp

## 20240911.1

Support AVITI protocol

## 20240815.1

Support Illumina DNA No-QC protocol

## 20240404.1

Add support for Smart-seq3 v1.0 protocol

## 20240125.1

Fix FA image for the OmniC Tissue and Lysate QC protocol

## 20231208.1

Update msg format in GA project mail

## 20231204.1

Aggregate QC for in-house libraries can be on a pool too just like finished libraries to get correct sequenced FC

## 20231114.1

Fix bug of wrong data type

## 20231109.1

Avoid overwriting order details when fetching from order portal fails

## 20231106.1

Fix document update conflict error with escalation RN

## 20231101.1

Add key names for new UDFs for setup ws

## 20231027.1

Fix bug with sample reference and index errors

## 20231024.1

Support workset with multiple replicates of the same sample

## 20231019.2

Clear hardcoded step ids

## 20231019.1

Add support for CytAssist Library Prep protocol

## 20231018.1

Add script to convert review comment in the Aggregate QC steps to running notes

## 20231012.1

Add missing sequencing step IDs

## 20231011.1

Add support for Illumina DNA PCR-free protocol

## 20230719.1

Support NovaSeqXPlus

## 20230315.1

Include control info for workset

## 20221025.1

Add Library prep option to workset key list

## 20220615.1

Re-define prepstart for the HiC protocol

## 20220610.1

Convert statusdb urls to https

## 20220427.1

Support additional 10X index types

## 20220412.1

Change 10X index pattern name

## 20220329.1

Also report workset prepared in the Applications Generic Protocol; Replace old sequencing step IDs

## 20220326.1

Define preprepstart and prepstart for the OmniC protocol

## 20211029.1

Refactor based on comments from AA

## 20211028.2

Fix bugs

## 20211028.1

Refactor escalations to report processid, requester and reviewer

## 20210707.1

Adjust classes.py so that sample well location can be reported before RC

## 20210702.1

Define prepstart, prepend and sequencing for the ONT protocol

## 20210617.1

Support additional 10X index types

## 20210525.2

Fix issue with error message

## 20210525.1

Use safeloader for yaml

## 20210520.1

Define prepstart and prepend for QIAseq miRNA protocol

## 20210422.1

Port scripts to support both python 2 and 3

## 20210412.1

Fix bug that new library construction methods for finished libraries cannot be handled

## 20210323.1

Fix bug that new workset cannot be imported with step id specified

## 20210318.2

Support additional 10X index types

## 20210318.1

Setup VERSIONLOG.md
