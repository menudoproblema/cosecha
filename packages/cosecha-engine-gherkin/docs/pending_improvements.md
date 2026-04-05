# Pending Improvements de Gherkin

- reducir el peor caso residual de ambigüedad cuando muchas
  definiciones compartan prefijos, sufijos y tokens ancla parecidos,
- seguir bajando el coste de import de módulos de definiciones grandes o
  con imports pesados,
- endurecer la caché negativa y los buckets de ambigüedad del
  `LazyStepResolver` en consumers grandes,
- empujar todavía más matching desde conocimiento persistido para evitar
  reconstrucciones locales innecesarias.
