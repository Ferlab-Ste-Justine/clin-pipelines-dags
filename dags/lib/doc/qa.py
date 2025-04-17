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
- Table gnomad_genomes_v4
- Table gnomad_joint_v4
- Table normalized_snv
- Table normalized_consequences
- Table normalized_variants
- Table consequences
- Table variants
- Table variant_centric
- Table cnv_centric
- Table coverage_by_gene
- Table nextflow_svclustering
- Table nextflow_svclustering_parental_origin

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
- Test : Non duplication - Table gnomad_genomes_v4
- Objectif : Les variants doivent être uniques dans la table gnomad_genomes_v4
'''

no_dup_gnomad_joint = '''
### Documentation
- Test : Non duplication - Table gnomad_joint_v4
- Objectif : Les variants doivent être uniques dans la table gnomad_joint_v4
'''

no_dup_clinical = '''
### Documentation
- Test : Non duplication - Table enriched_clinical
- Objectif : L'information clinique doit être unique par analysis_service_request_id, service_request_id et bioinfo_analysis_code dans la table enriched_clinical
'''

no_dup_nor_snv = '''
### Documentation
- Test : Non duplication - Table normalized_snv
- Objectif : Les variants doivent être uniques par service_request_id dans la table normalized_snv
'''

no_dup_nor_snv_somatic = '''
### Documentation
- Test : Non duplication - Table normalized_snv_somatic
- Objectif : Les variants doivent être uniques par service_request_id dans la table normalized_snv_somatic
'''

no_dup_nor_consequences = '''
### Documentation
- Test : Non duplication - Table normalized_consequences
- Objectif : Les conséquences doivent être uniques dans la table normalized_consequences
'''

no_dup_nor_variants = '''
### Documentation
- Test : Non duplication - Table normalized_variants
- Objectif : Les variants doivent être uniques par batch_id dans la table normalized_variants
'''

no_dup_snv = '''
### Documentation
- Test : Non duplication - Table snv
- Objectif : Les variants doivent être uniques par service_request_id dans la table snv
'''

no_dup_snv_somatic = '''
### Documentation
- Test : Non duplication - Table snv_somatic
- Objectif : Les variants doivent être uniques par service_request_id dans la table snv_somatic
'''

no_dup_consequences = '''
### Documentation
- Test : Non duplication - Table consequences
- Objectif : Les conséquences doivent être uniques dans la table consequences
'''

no_dup_variants = '''
### Documentation
- Test : Non duplication - Table variants
- Objectif : Les variants doivent être uniques dans la table variants
'''

no_dup_variant_centric = '''
### Documentation
- Test : Non duplication - Table variant_centric
- Objectif : Les variants doivent être uniques dans la table variant_centric
'''

no_dup_cnv_centric = '''
### Documentation
- Test : Non duplication - Table cnv_centric
- Objectif : Les CNVs doivent être uniques dans la table cnv_centric
'''

no_dup_coverage_by_gene = '''
### Documentation
- Test : Non duplication - Table coverage_by_gene
- Objectif : Les coverages doivent être uniques dans la table coverage_by_gene
'''

no_dup_nextflow_svclustering = '''
### Documentation
- Test : Non duplication - Table nextflow_svclustering
- Objectif : Les clusters doivent être uniques dans la table nextflow_svclustering
'''

no_dup_nextflow_svclustering_parental_origin = '''
### Documentation
- Test : Non duplication - Table nextflow_svclustering_parental_origin
- Objectif : Les clusters doivent être uniques par service_request_id dans la table nextflow_svclustering_parental_origin
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
