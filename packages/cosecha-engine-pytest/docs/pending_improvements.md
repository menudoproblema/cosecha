# Pending Improvements de Pytest

- definir una estrategia de discovery estático todavía más eficiente
  para suites grandes,
- extender la resolución fuera del dominio visible actual sin perder
  identidad estable ni capacidad de explain,
- ampliar la parametrización más allá del subconjunto literal soportado,
- ampliar la evaluación estática de `skipif` y `xfail` cuando hoy solo
  cabe degradar a runtime real,
- endurecer el batching del adaptador real de `pytest`,
- enriquecer el conocimiento tipado sobre plugins externos y fixtures
  publicadas por librerías auxiliares.
