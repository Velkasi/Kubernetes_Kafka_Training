# Contrat Avro

`simulation-measure.avsc` est créé à l'**étape 11** du guide, à partir du schéma que la source A a
enregistré elle-même dans Schema Registry :

```bash
curl -s http://localhost:8081/subjects/lab.simulation.measures-value/versions/latest/schema \
  > schemas/simulation-measure.avsc
```

Il n'est pas fourni d'avance : un `.avsc` écrit à la main ne correspond presque jamais, à l'octet
près, à ce que génère le connecteur JDBC. C'est ce fichier que tu reprendras en prod.
