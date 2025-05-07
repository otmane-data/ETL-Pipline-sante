🏥 ETL Pipeline Santé
<p align="center"> <img src="assets/Animation.gif" alt="Pipeline ETL Animation"> </p>
Ce projet implémente un pipeline ETL complet pour le traitement et l’analyse des données de santé. Il s’appuie sur des outils modernes tels qu’Airflow, Grafana, Prometheus, et Superset pour l’orchestration, la visualisation et le monitoring des données.

📌 Sommaire
🧱 Architecture

⚙️ Prérequis

🚀 Installation

🛠️ Configuration

📂 Base de données

🌬️ Airflow

📊 Grafana

📈 Superset

🔄 Pipeline de Données

📉 Métriques

🧩 Monitoring

🧼 Maintenance

🔐 Sécurité

📬 Support

🧱 Architecture
Apache Airflow : Orchestration des tâches ETL.

Grafana : Visualisation des métriques de performance.

Prometheus : Collecte et stockage des métriques.

Superset : Analyse et exploration des données de santé.

StatsD : Agrégation et export des métriques vers Prometheus.

⚙️ Prérequis
Docker & Docker Compose

Python ≥ 3.8

PostgreSQL

🚀 Installation
Cloner le repository :

bash
Copier
Modifier
git clone <url-du-repo>
cd etl-pipeline-sante
Installer les dépendances Python :

bash
Copier
Modifier
pip install -r requirements.txt
Lancer les services avec Docker :

bash
Copier
Modifier
docker-compose up -d
🛠️ Configuration
📂 Base de données
Initialiser la base de production :

bash
Copier
Modifier
psql -f init-prod-sante.sql
Initialiser la base analytique :

bash
Copier
Modifier
psql -f init-analytics-sante.sql
🌬️ Airflow
Interface : http://localhost:8084

DAGs disponibles dans le dossier Dags/

Configuration personnalisable dans airflow.cfg

📊 Grafana
Interface : http://localhost:3000

Dashboards configurés via Grafana/provisioning/Dashboards/

📈 Superset
Interface : http://localhost:8088

Dashboards disponibles dans superset/dashboards/

🔄 Pipeline de Données
Extraction : Requête des données depuis la base de production.

Transformation : Nettoyage, agrégation et enrichissement des données.

Chargement : Insertion des données transformées dans la base analytique.

📉 Métriques
Quelques exemples de métriques surveillées :

🔹 Taux d'occupation des établissements

🔹 Coût moyen des consultations

🔹 Performance globale du pipeline ETL

🧩 Monitoring
🔎 Dashboards de performance via Grafana

📊 Analyse des données via Superset

📋 Logs d’exécution dans Airflow

🧼 Maintenance
Vérification régulière des logs Airflow

Suivi de la santé du pipeline via les métriques Grafana

Maintenance et sauvegarde des bases de données

🔐 Sécurité
Accès sécurisé aux interfaces web

Chiffrement des données sensibles

Gestion des rôles et des permissions
