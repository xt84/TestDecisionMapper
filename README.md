# Test task for DecisionMapper

For run:

```
$ spark-submit \
--packages com.databricks:spark-csv_2.11:1.5.0 \
--class xt84.info.decisionmapper.csvtrs.Main </path/to/jar/dir>/TestDecisionMapper-1.0-SNAPSHOT.jar \
--data </path/to/dataset/dir/<dataset-file-name>.csv \
--rules </path/to/rules/dir/<rules-file-name>.json \
--output </path/to/transformed/dataset/dir> \
--report </path/to/report/dir>/<report-file-name>.json
```