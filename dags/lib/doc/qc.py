etl_qc = '''
# Tests non critiques

L'échec d'un de ces tests ne bloque pas l'exécution du DAG.

## Série de tests validant les variants des batchs à partir du fichier VCF

### Fonctionnement des tests
- Filtrer les variants provenant du fichier VCF de la batch
  - chromosome = {1 à 22, X, Y}
  - alternate != *
- Vérifier que tous les variants sont dans les tables

### Différents tests
- Table normalized_snv
- Table normalized_variants
- Table normalized_snv_somatic

## Série de tests validant les filtres sur les variants

### Différents filtres appliqués
- Table normalized_snv : Les variants du fichier VCF sont filtrés pour ne garder que les variants dont alternate != *
- Table normalized_variants : Les fréquences sont calculées uniquement si gq >= 20, filter contient PASS, ad_alt >= 3 et alternate != *

### Fonctionnement des tests
- Filtrer les variants selon le filtre applicable
- Vérifier qu’aucun variant indésirable n'est dans les tables
- Vérifier qu’aucun variant n’a été oublié ou est en trop pour le calcul des fréquences selon gq, filters, ad_alt et alternate

### Différents tests
- Filtre alternate sur la table normalized_snv
- Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (en trop)
- Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (oubliés)

## Série de tests sur la diversité des colonnes des tables centric

### Fonctionnement des tests
- Se positionner dans le schéma de la table
- Parcourir la “white-list” ou exclure la “black-list” des colonnes accessibles directement sous le niveau où on est positionné
- Pour chaque colonne parcourue, filtrer les données selon le test
- Vérifier que les colonnes respectent le test

### Différents tests
- Les colonnes de données sans null
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric
- Les colonnes de données entièrement null
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric
- Les colonnes de données à valeur unique
  - Table variant_centric
  - Table gene_centric
  - Table cnv_centric

## Série de tests sur les dictionnaires

### Fonctionnement des tests
- Vérifier que les valeurs d'une colonne sont incluses dans son dictionnaire

### Différents tests
- Les colonnes de cnv_centric
- Les colonnes de variant_centric.consequences
- Les colonnes de variant_centric.donors
- Les colonnes de variant_centric

## Série de tests validant le calcul des fréquences

### Définitions des calculs
Les variants considérés dans le calcul des fréquences sont ceux ayant le filtre Dragen à PASS et ayant un GQ >= 20
- ac: Allele Count<span style="color:white">-----------------</span>- Nombre d'allèles ayant le variant
- an: Allele Number<span style="color:white">--------------</span>- Nombre d'allèles total (= 2 pn)
- af: Allele Frequency<span style="color:white">----------</span>- Ratio ac/an
- pc: Participant Count<span style="color:white">--------</span>- Nombre de participants ayant le variant
- pn: Participant Number<span style="color:white">-----</span>- Nombre de participants total séquencés
- pf: Participant Frequency<span style="color:white">-</span>- Ratio pc/pn

### Fonctionnement des tests
- Caculer les valeurs attendues de ac, an, pc et pn à partir des variants de la table normalized_snv
- Récupérer et comparer les valeurs de ac, an, pc et pn des variants de la table variant_centric

### Différents tests
- frequency_RQDM - total
- frequency_RQDM - affected
- frequency_RQDM - non_affected
- frequencies_by_analysis - total
- frequencies_by_analysis - affected
- frequencies_by_analysis - non_affected

---
Pour plus de détails sur chaque test, voir "Task Instance Details".
'''

vcf_snv = '''
### Documentation
- Test : Table normalized_snv
- Objectif : La liste filtrée des variants du fichier VCF est la même que dans la table normalized_snv
'''

vcf_nor_variants = '''
### Documentation
- Test : Table normalized_variants
- Objectif : La liste filtrée des variants du fichier VCF est la même que dans la table normalized_variants
'''

vcf_snv_somatic = '''
### Documentation
- Test : Table normalized_snv_somatic
- Objectif : La liste filtrée des variants du fichier VCF est la même que dans la table normalized_snv_somatic
'''

filters_snv = '''
### Documentation
- Test : Filtre alternate sur la table normalized_snv
- Objectif : Aucun variant dans la table normalized_snv n’a alternate = *
'''

filters_frequency_extra = '''
### Documentation
- Test : Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (en trop)
- Objectif : Tous les variants qui ne satisfont pas gq >= 20, filters = PASS, ad_alt >= 3 et alternate = * n’ont pas de calcul des fréquences
'''

filters_frequency_missed = '''
### Documentation
- Test : Filtre gq, filters, ad_alt et alternate sur la table normalized_variants (oubliés)
- Objectif : Tous les variants qui satisfont gq >= 20, filters = PASS, ad_alt >= 3 et alternate = * ont un calcul des fréquences
'''

no_null_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_variant_centric_donors = '''
### Documentation
- Test : Table variant_centric sous donors - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_variant_centric_freqbyanal = '''
### Documentation
- Test : Table variant_centric sous frequencies_by_analysis - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_variant_centric_freqrqdm = '''
### Documentation
- Test : Table variant_centric sous frequency_RQDM - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

no_null_coverage_by_gene = '''
### Documentation
- Test : Table coverage_by_gene - Les colonnes de données sans null
- Objectif : Les données dans les colonnes spécifiées ne contiennent pas de null
'''

only_null_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_cmc = '''
### Documentation
- Test : Table variant_centric sous cmc - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_clinvar = '''
### Documentation
- Test : Table variant_centric sous clinvar - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_consequences = '''
### Documentation
- Test : Table variant_centric sous consequences - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_donors = '''
### Documentation
- Test : Table variant_centric sous donors - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_exomax = '''
### Documentation
- Test : Table variant_centric sous exomiser_max - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_extfreq = '''
### Documentation
- Test : Table variant_centric sous external_frequencies - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_framax = '''
### Documentation
- Test : Table variant_centric sous franklin_max - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_freqbyanal = '''
### Documentation
- Test : Table variant_centric sous frequencies_by_analysis - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_freqrqdm = '''
### Documentation
- Test : Table variant_centric sous frequency_RQDM - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_variant_centric_freqrqdmtumor = '''
### Documentation
- Test : Table variant_centric sous freq_rqdm_tumor_only et freq_rqdm_tumor_normal - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

only_null_coverage_by_gene = '''
### Documentation
- Test : Table coverage_by_gene - Les colonnes de données entièrement null
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes null
'''

same_value_variant_centric = '''
### Documentation
- Test : Table variant_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_cmc = '''
### Documentation
- Test : Table variant_centric sous cmc - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_clinvar = '''
### Documentation
- Test : Table variant_centric sous clinvar - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_consequences = '''
### Documentation
- Test : Table variant_centric sous consequences - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_donors = '''
### Documentation
- Test : Table variant_centric sous donors - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_exomax = '''
### Documentation
- Test : Table variant_centric sous exomiser_max - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_extfreq = '''
### Documentation
- Test : Table variant_centric sous external_frequencies - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_framax = '''
### Documentation
- Test : Table variant_centric sous franklin_max - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_freqbyanal = '''
### Documentation
- Test : Table variant_centric sous frequencies_by_analysis - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_freqrqdm = '''
### Documentation
- Test : Table variant_centric sous frequency_RQDM - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_variant_centric_freqrqdmtumor = '''
### Documentation
- Test : Table variant_centric sous freq_rqdm_tumor_only et freq_rqdm_tumor_normal - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_gene_centric = '''
### Documentation
- Test : Table gene_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_cnv_centric = '''
### Documentation
- Test : Table cnv_centric - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

same_value_coverage_by_gene = '''
### Documentation
- Test : Table coverage_by_gene - Les colonnes de données à valeur unique
- Objectif : Les données dans les colonnes (sauf celles spécifiées) ne sont pas toutes de la même valeur
'''

dictionary_cnv = '''
### Documentation
- Test : Table cnv_centric
- Objectif : Les valeurs des colonnes sont incluses dans leur dictionnaire
'''

dictionary_consequences = '''
### Documentation
- Test : Table variant_centric.consequences
- Objectif : Les valeurs des colonnes sont incluses dans leur dictionnaire
'''

dictionary_donors = '''
### Documentation
- Test : Table variant_centric.donors
- Objectif : Les valeurs des colonnes sont incluses dans leur dictionnaire
'''

dictionary_snv = '''
### Documentation
- Test : Table variant_centric
- Objectif : Les valeurs des colonnes sont incluses dans leur dictionnaire
'''

freq_rqdm_total = '''
### Documentation
- Test : Calcul de frequency_RQDM - total
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequency_RQDM.total sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_rqdm_affected = '''
### Documentation
- Test : Calcul de frequency_RQDM - affected
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequency_RQDM.affected sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_rqdm_non_affected = '''
### Documentation
- Test : Calcul de frequency_RQDM - non_affected
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequency_RQDM.non_affected sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_by_analysis_total = '''
### Documentation
- Test : Calcul de frequencies_by_analysis - total
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequencies_by_analysis.total sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_by_analysis_affected = '''
### Documentation
- Test : Calcul de frequencies_by_analysis - affected
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequencies_by_analysis.affected sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_by_analysis_non_affected = '''
### Documentation
- Test : Calcul de frequencies_by_analysis - non_affected
- Objectif : Les valeurs de pc, pn, ac et an pour variant_centric.frequencies_by_analysis.non_affected sont bien calculées par rapport aux données de la table normalized_snv
'''

freq_rqdm_tumor_only = '''
### Documentation
- Test : Calcul de freq_rqdm_tumor_only
- Objectif : Les valeurs de pc et pn pour variant_centric.freq_rqdm_tumor_only sont bien calculées par rapport aux données de la table normalized_snv_somatic
'''

freq_rqdm_tumor_normal = '''
### Documentation
- Test : Calcul de freq_rqdm_tumor_normal
- Objectif : Les valeurs de pc et pn pour variant_centric.freq_rqdm_tumor_normal sont bien calculées par rapport aux données de la table normalized_snv_somatic
'''

