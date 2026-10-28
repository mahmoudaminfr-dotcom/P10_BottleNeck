# 🍷 BottleNeck — Pipeline d'Orchestration & Réconciliation de Données (Kestra / DuckDB / Python)

## 1. Contexte & Objectifs Métier
**BottleNeck**, marchand de vin spécialisé, dispose de deux systèmes d'information distincts :
* Un système **ERP** gérant les références produits, les prix unitaires et les stocks (`Fichier_erp.xlsx`).
* Une plateforme e-commerce **CMS** centralisant les ventes en ligne et les métadonnées produits (`Fichier_web.xlsx`).
* Une table de correspondance permettant le rapprochement des identifiants (`fichier_liaison.xlsx`).

Ce projet industrialise et automatise le pipeline mensuel d'intégration, de nettoyage, de contrôle qualité (*Fail-Fast*) et d'analyse des données afin de générer automatiquement **chaque 15 du mois à 09h00 (heure de Paris)** en mode production (`PRODUCTION_MENSUELLE`) :
1. **`rapport_chiffre_affaires.xlsx`** : Rapport financier Excel à deux onglets (`CA_par_produit` et `CA_Total`) destiné au Pilotage & à la Direction.
2. **`vins_premium.csv`** : Extraction des 30 vins millésimes identifiés statistiquement par la méthode du Z-score ($z > 2$) pour le ciblage B2B (Laure).
3. **`vins_ordinaires.csv`** : Extraction des 684 vins courants ($z \le 2$) pour les campagnes B2C (Maria).

---

## 2. Architecture du Pipeline & Stratégie de Tests (Fail-Fast)

L'orchestration est assurée par **Kestra** et sépare strictement la logique d'ordonnancement du traitement de la donnée, délégué à **DuckDB** (SQL analytique) et **Python** (`pandas`).

* **Calculs strictement séquencés :** Afin de respecter le verrou d'écriture unique (*Single-Writer Lock*) de DuckDB, toutes les étapes de préparation, de réconciliation, de calcul métier (4A puis 4B) et leurs tests associés s'exécutent l'une après l'autre dans un dossier de travail isolé (`.staging_<execution.id>`).
* **Exports parallélisés :** Une fois les tables analytiques validées en base, seuls les exports de fichiers (Excel et CSV) s'exécutent simultanément en parallèle (`io.kestra.plugin.core.flow.Parallel`) en lecture seule (`read_only=True`).
* **Publication atomique & Pointeur :** Après validation finale des fichiers générés par le **Test 5**, le dossier de staging est publié sous forme de lot versionné (`data/outputs/lots/lot_<execution.id>/`) et le fichier repère **`data/outputs/LAST_VALID_LOT.txt`** est mis à jour atomiquement (`os.replace`) pour indiquer le chemin du dernier lot certifié.
* **Gestion des erreurs & Alerting :** En cas d'échec d'une tâche ou d'un contrôle qualité (Tests 1 à 5), le pipeline s'arrête immédiatement, purge le dossier `.staging_<execution.id>` et inscrit **une ligne d'alerte critique directement dans les logs d'exécution Kestra** (sans notification externe envoyée à l'équipe). Le dernier lot certifié pointé par `LAST_VALID_LOT.txt` reste ainsi 100 % intact.

![Architecture du Pipeline Kestra BottleNeck](Mahmoud_Amin_1_diagramme_102026.drawio.png)

### Synthèse des contrôles qualité et métriques de référence

| Étape | Traitement exécuté | Test Automatisé (Fail-Fast) | Résultat validé (Recette) |
| :--- | :--- | :--- | :--- |
| **1. Ingestion & Nettoyage** *(Séquentiel)* | Contrôle de présence/taille des sources `.xlsx`, création du staging isolé, typage `VARCHAR` (`id_web`, `sku`), filtrage des clés `NULL` | **Test 1 :** `0 NULL` sur les clés primaires et contrôle volumétrique `web_clean` | `erp`: 825 \| `liaison`: 825 \| `web`: **1 428 lignes** |
| **2. Dédoublonnage SQL** *(Séquentiel)* | Unicité sur `product_id` et filtrage `post_type = 'product'` + unicité sur `sku` | **Test 2 :** `COUNT(*) == COUNT(DISTINCT clé)` sur les 3 tables | `erp`: **825** \| `liaison`: **825** \| `web`: **714 lignes** |
| **3. Réconciliation** *(Séquentiel)* | Double `INNER JOIN` SQL (`erp_dedup` $\leftrightarrow$ `liaison_dedup` $\leftrightarrow$ `web_dedup`) | **Test 3 :** Volumétrie exacte, `0 NULL` sur `price`/`total_sales`, `price > 0` | **714 produits** réconciliés |
| **4A. Calcul du CA** *(Séquentiel)* | Calcul SQL du CA par produit (`ROUND(price * total_sales, 2)`) et du CA global | **Test 4A :** Aucun CA négatif, formule exacte et contrôle du montant total agrégé | **70 568,60 €** (sur 714 produits) |
| **4B. Segmentation** *(Séquentiel)* | Calcul Python du Z-score sur les prix : $z = \frac{x - \mu}{\sigma}$ ($\text{ddof}=0$) | **Test 4B :** `0 NULL`/`NaN` et vérification de la partition complète ($30 + 684 = 714$) | **30 Vins Premium** ($z > 2$) \| **684 Vins Ordinaires** ($z \le 2$) |
| **5. Exports simultanés** *(Parallèle)* | Lectures DuckDB (`read_only=True`) et création simultanée du fichier `.xlsx` (Branche 1) et des 2 fichiers `.csv` (Branche 2) en staging | **Test 5 (Final) :** Vérification physique des fichiers, `0 NaN`/`NULL` sur champs obligatoires Excel, égalité Somme CA / DuckDB, unicité et partition des IDs CSV | Fichiers certifiés et publiés dans `lots/lot_<id>/` via **`LAST_VALID_LOT.txt`** |

### Anomalies métier identifiées lors de l'audit des sources
* **Références ERP à prix négatifs :** 2 références (`product_id = 5017` à `-8,00 €` et `product_id = 6594` à `-1,00 €`) figurent dans l'ERP avec `onsale_web = 0` et sans `id_web`. Elles sont écartées par la jointure interne et surveillées par le **Test 3** (`price > 0`).
* **Produits CMS sans clé `sku` :** Sur les 85 lignes sans `sku` dans `Fichier_web.xlsx`, 2 lignes correspondent à des fiches produits actives (*Pierre Jean Villa Condrieu Jardin Suspendu 2018* et *Pierre Jean Villa Côte Rôtie Fongeant 2017*) devant être corrigées dans le CMS.

---

## 3. Structure du Dépôt & Accès aux Livrables

Les fichiers générés ne sont jamais écrasés directement à la racine de `data/outputs/`. Pour récupérer les derniers fichiers valides, il faut lire le pointeur **`data/outputs/LAST_VALID_LOT.txt`** qui indique le sous-dossier actif dans `data/outputs/lots/lot_<execution_id>/` :

```text
.
├── data/
│   ├── inputs/                                    # Exports sources (.xlsx : ERP, Liaison, Web)
│   └── outputs/                                   # Sorties gérées par publication atomique
│       ├── LAST_VALID_LOT.txt                     # Pointeur indiquant le dernier lot certifié
│       └── lots/
│           └── lot_<execution_id>/                # Dossier immuable du lot validé par le Test 5
│               ├── bottleneck.duckdb              # Base analytique DuckDB consolidée du lot
│               ├── rapport_chiffre_affaires.xlsx  # Rapport financier (CA_par_produit, CA_Total)
│               ├── vins_premium.csv               # Extraction des 30 vins millésimes (z > 2)
│               └── vins_ordinaires.csv            # Extraction des 684 vins ordinaires (z <= 2)
├── .env.example                                   # Modèle des variables d'environnement Docker/Postgres
├── docker-compose.yml                             # Stack Docker (Kestra Standalone + PostgreSQL 16)
├── Mahmoud_Amin_1_diagramme_102026.drawio         # Logigramme d'architecture (Draw.io)
├── Mahmoud_Amin_1_diagramme_102026.drawio.png     # Export visuel de l'architecture
├── Mahmoud_Amin_2_workflow_102026.yaml            # Workflow Kestra déclaratif & tests intégrés
└── README.md                                      # Documentation technique
```

---

## 4. Déploiement & Exécution

```bash
# 1. Créer le fichier d'environnement à partir du modèle fourni
cp .env.example .env

# 2. Démarrer l'infrastructure Docker (Kestra + PostgreSQL 16 avec healthcheck)
docker compose up -d

# 3. Accéder à l'interface Kestra sur http://localhost:8080
# 4. Importer le flux Mahmoud_Amin_2_workflow_102026.yaml et lancer une exécution
```

* **Exécution manuelle (Recette ou Production) :** Depuis l'interface Kestra, choisir le paramètre `mode_validation` (`RECETTE_INITIALE` pour vérifier les seuils stricts du jeu d'essai ou `PRODUCTION_MENSUELLE` pour les invariants dynamiques).
* **Exécution planifiée (Trigger) :** Le trigger `planification_mensuelle_15_du_mois` lance automatiquement le flux en mode `PRODUCTION_MENSUELLE` selon l'expression Cron `0 9 15 * *` (`timezone: Europe/Paris`).