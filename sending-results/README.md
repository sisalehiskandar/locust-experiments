```
curl -XPUT "http://localhost:9200/_template/dev" -H 'Content-Type: application/json' -d'{"index_patterns": "*","settings": {"number_of_shards": 1}}'
```