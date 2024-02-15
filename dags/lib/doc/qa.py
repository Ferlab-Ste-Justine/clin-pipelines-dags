etl_qa = '''
# Tests critiques

L'échec d'un de ces tests bloque l'exécution du DAG.

## Série de tests sur les tables

### Fonctionnement des tests
- Tester chaque table
- Vérifier que les colonnes respectent le test

### Différents tests
- Tables non vides

## Série de tests sur la non duplication des entités dans les tables

### Fonctionnement des tests
- Regrouper les entités selon une clé unique
- Vérifier qu'aucune entité n'est dupliquée

### Différents tests
- Table gnomad_genomes_v3
- Table normalized_snv
- Table normalized_consequences
- Table normalized_variants
- Table consequences
- Table variants
- Table variant_centric
- Table cnv_centric
- Table varsome

## Série de tests comparant la liste des variants entre les tables

### Fonctionnement des tests
- Lister les variants distincts selon la clé (chromosome, start, reference, alternate)
- Vérifier que la liste des variants des deux tables est identique

### Différents tests
- Entre les tables normalized_snv et normalized_variants
- Entre les tables normalized_snv et variants
- Entre les tables variants et variant_centric

---
Pour plus de détails sur chaque test, voir "Task Instance Details".
'''

non_empty_tables = '''
### Documentation
- Test : Tables non vides
- Objectif : Les tables ne doivent pas être vide
'''

no_dup_gnomad = '''
### Documentation
- Test : Non duplication - Table gnomad_genomes_v3
- Objectif : Les variants doivent être unique dans la table gnomad_genomes_v3
'''

no_dup_nor_snv = '''
### Documentation
- Test : Non duplication - Table normalized_snv
- Objectif : Les variants doivent être unique par service_request_id dans la table normalized_snv
'''

no_dup_nor_snv_somatic = '''
### Documentation
- Test : Non duplication - Table normalized_snv_somatic
- Objectif : Les variants doivent être unique par service_request_id dans la table normalized_snv_somatic
'''

no_dup_nor_consequences = '''
### Documentation
- Test : Non duplication - Table normalized_consequences
- Objectif : Les conséquences doivent être unique dans la table normalized_consequences
'''

no_dup_nor_variants = '''
### Documentation
- Test : Non duplication - Table normalized_variants
- Objectif : Les variants doivent être unique par batch_id dans la table normalized_variants
'''

no_dup_snv = '''
### Documentation
- Test : Non duplication - Table snv
- Objectif : Les variants doivent être unique par service_request_id dans la table snv
'''

no_dup_snv_somatic = '''
### Documentation
- Test : Non duplication - Table snv_somatic
- Objectif : Les variants doivent être unique par service_request_id dans la table snv_somatic
'''

no_dup_consequences = '''
### Documentation
- Test : Non duplication - Table consequences
- Objectif : Les conséquences doivent être unique dans la table consequences
'''

no_dup_variants = '''
### Documentation
- Test : Non duplication - Table variants
- Objectif : Les variants doivent être unique dans la table variants
'''

no_dup_variant_centric = '''
### Documentation
- Test : Non duplication - Table variant_centric
- Objectif : Les variants doivent être unique dans la table variant_centric
'''

no_dup_cnv_centric = '''
### Documentation
- Test : Non duplication - Table cnv_centric
- Objectif : Les CNVs doivent être unique dans la table cnv_centric
'''

no_dup_varsome = '''
### Documentation
- Test : Non duplication - Table varsome
- Objectif : Les variants doivent être unique dans la table varsome
'''

same_list_nor_snv_nor_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables normalized_snv et normalized_variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table normalized_snv est incluse dans celle de la table normalized_variants
'''

same_list_nor_snv_somatic_nor_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables normalized_snv_somatic et normalized_variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table normalized_snv_somatic est incluse dans celle de la table normalized_variants
'''

same_list_snv_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables snv et variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table snv est incluse dans celle de la table variants
'''

same_list_snv_somatic_variants = '''
### Documentation
- Test : Liste des variants - Entre les tables snv_somatic et variants
- Objectif : La liste des variants ayant ad_alt >= 3 dans la table snv_somatic est incluse dans celle de la table variants
'''

same_list_variants_variant_centric = '''
### Documentation
- Test : Liste des variants - Entre les tables variants et variant_centric
- Objectif : La liste des variants dans la table variants est la même que dans la table variant_centric
'''
